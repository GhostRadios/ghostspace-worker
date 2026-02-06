import express from "express";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import dns from "node:dns";
import { Agent, setGlobalDispatcher } from "undici";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

dns.setDefaultResultOrder("ipv4first");
setGlobalDispatcher(new Agent({ connect: { family: 4 } }));

const app = express();
const PORT = process.env.PORT || 8080;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const BUCKET_SOURCE = "videos";
const BUCKET_HLS = "videos-hls";
const WORKER_ID = "ghostspace-worker-new";

let isWorking = false;

app.get("/", (_req, res) => {
  res.json({ ok: true, service: "ghostspace-worker" });
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function signedUrl(bucket, objectPath, expiresInSec = 3600) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .createSignedUrl(objectPath, expiresInSec);
  if (error || !data?.signedUrl) {
    throw new Error(`signed url error: ${error?.message || "unknown"}`);
  }
  return data.signedUrl;
}

async function downloadToFile(url, destPath, retries = 3) {
  let lastErr;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const res = await fetch(url);
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(
          `http ${res.status} ${res.statusText} ${text.slice(0, 200)}`
        );
      }
      if (!res.body) throw new Error("empty response body");
      await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(destPath));
      return;
    } catch (err) {
      lastErr = err;
      console.warn(
        `[DL] attempt ${attempt}/${retries} failed: ${err?.message || err}`
      );
      await sleep(1000 * attempt);
    }
  }
  throw lastErr;
}

async function uploadFolder(bucket, prefix, folderPath) {
  const files = fs.readdirSync(folderPath);
  for (const file of files) {
    const full = path.join(folderPath, file);
    const stat = fs.statSync(full);
    if (stat.isDirectory()) {
      await uploadFolder(bucket, `${prefix}/${file}`, full);
    } else {
      const content = fs.readFileSync(full);
      const { error } = await supabase.storage
        .from(bucket)
        .upload(`${prefix}/${file}`, content, {
          contentType: file.endsWith(".m3u8")
            ? "application/vnd.apple.mpegurl"
            : "video/MP2T",
          upsert: true,
        });
      if (error) throw error;
    }
  }
}

async function transcodeToHls(inputFile, outDir) {
  await new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", [
      "-y",
      "-i",
      inputFile,
      "-preset",
      "veryfast",
      "-g",
      "48",
      "-sc_threshold",
      "0",
      "-hls_time",
      "2",
      "-hls_playlist_type",
      "vod",
      "-hls_segment_filename",
      path.join(outDir, "segment_%03d.ts"),
      path.join(outDir, "index.m3u8"),
    ]);

    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg failed with code ${code}`));
    });
  });
}

async function processJob(job) {
  const { id, post_id, source_path } = job;
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gs-"));
  const outDir = path.join(tmpDir, "hls");
  const sourceFile = path.join(tmpDir, "source.mp4");
  fs.mkdirSync(outDir, { recursive: true });

  try {
    const url = await signedUrl(BUCKET_SOURCE, source_path);
    console.log(`[JOB] Signed URL: ${url}`);

    console.log(`[JOB] Downloading ${source_path}`);
    await downloadToFile(url, sourceFile);

    const hlsPrefix = `${post_id}/${id}`;
    console.log(`[JOB] Transcoding ${source_path} -> ${hlsPrefix}`);
    await transcodeToHls(sourceFile, outDir);

    console.log(`[JOB] Uploading HLS to ${BUCKET_HLS}/${hlsPrefix}`);
    await uploadFolder(BUCKET_HLS, hlsPrefix, outDir);

    const hlsPath = `${hlsPrefix}/index.m3u8`;
    const hlsPublicUrl = `${SUPABASE_URL}/storage/v1/object/public/${BUCKET_HLS}/${hlsPath}`;

    await Promise.all([
      supabase.from("posts").update({ hls_path: hlsPath, media_url: hlsPublicUrl }).eq("id", post_id),
      supabase.from("gs_posts").update({ hls_path: hlsPath, media_url: hlsPublicUrl }).eq("id", post_id),
    ]);

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "completed",
        last_error: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", id);
  } catch (err) {
    const msg = err?.message || String(err);
    console.error("Worker error:", msg);
    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "failed",
        last_error: msg.slice(0, 500),
        updated_at: new Date().toISOString(),
      })
      .eq("id", id);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

async function poll() {
  if (isWorking) return;
  isWorking = true;

  try {
    const { data: jobs, error } = await supabase
      .from("video_transcode_jobs")
      .select("*")
      .eq("status", "pending")
      .order("created_at", { ascending: true })
      .limit(1);

    if (error) throw error;
    if (!jobs || jobs.length === 0) return;

    const job = jobs[0];
    const now = new Date().toISOString();
    const lease = new Date(Date.now() + 30 * 60 * 1000).toISOString();

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "processing",
        locked_by: WORKER_ID,
        locked_at: now,
        lease_expires_at: lease,
      })
      .eq("id", job.id)
      .eq("status", "pending");

    await processJob(job);
  } catch (err) {
    console.error("Poll error:", err?.message || err);
  } finally {
    isWorking = false;
  }
}

setInterval(poll, 5000);

app.listen(PORT, () => {
  console.log(`Worker listening on ${PORT}`);
});
