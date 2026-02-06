import express from "express";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import { createWriteStream, promises as fsp } from "node:fs";
import path from "node:path";
import os from "node:os";
import dns from "node:dns";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

dns.setDefaultResultOrder("ipv4first");

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

const SRC_BUCKET = "videos";
const HLS_BUCKET = "videos-hls";
const WORKER_ID = "ghostspace-worker-new";
const POLL_MS = 5000;
const MAX_DOWNLOAD_RETRIES = 3;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

app.get("/", (_req, res) => {
  res.json({ ok: true, service: "ghostspace-worker" });
});

async function uploadFolder(bucket, prefix, folderPath) {
  const entries = await fsp.readdir(folderPath, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(folderPath, entry.name);
    if (entry.isDirectory()) {
      await uploadFolder(bucket, `${prefix}/${entry.name}`, full);
      continue;
    }
    const content = await fsp.readFile(full);
    const contentType = entry.name.endsWith(".m3u8")
      ? "application/vnd.apple.mpegurl"
      : "video/MP2T";

    const { error } = await supabase.storage
      .from(bucket)
      .upload(`${prefix}/${entry.name}`, content, {
        contentType,
        upsert: true,
      });

    if (error) throw error;
  }
}

async function downloadWithRetry(url, destPath) {
  for (let attempt = 1; attempt <= MAX_DOWNLOAD_RETRIES; attempt++) {
    try {
      const res = await fetch(url, { redirect: "follow" });
      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`);
      }
      await pipeline(Readable.fromWeb(res.body), createWriteStream(destPath));
      return;
    } catch (err) {
      if (attempt === MAX_DOWNLOAD_RETRIES) throw err;
      console.log(`[JOB] Download failed (attempt ${attempt}). Retrying...`);
      await sleep(2000 * attempt);
    }
  }
}

async function transcodeToHls(inputFile, outDir) {
  await new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", [
      "-y",
      "-i", inputFile,
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-profile:v", "main",
      "-level", "3.1",
      "-c:a", "aac",
      "-b:a", "128k",
      "-ac", "2",
      "-ar", "48000",
      "-g", "48",
      "-keyint_min", "48",
      "-sc_threshold", "0",
      "-hls_time", "2",
      "-hls_playlist_type", "vod",
      "-hls_segment_filename", path.join(outDir, "segment_%03d.ts"),
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
  const { id, post_id, source_path, attempts = 0 } = job;

  const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "gs-"));
  const inFile = path.join(tmpDir, "source.mp4");
  const outDir = path.join(tmpDir, "hls");

  try {
    await fsp.mkdir(outDir, { recursive: true });

    const { data: signed, error: signedErr } = await supabase.storage
      .from(SRC_BUCKET)
      .createSignedUrl(source_path, 3600);

    const downloadUrl =
      signed?.signedUrl ||
      `${SUPABASE_URL}/storage/v1/object/public/${SRC_BUCKET}/${source_path}`;

    if (signedErr) {
      console.log("[JOB] Signed URL error, using public URL fallback.");
    }

    console.log(`[JOB] Downloading ${source_path}`);
    await downloadWithRetry(downloadUrl, inFile);

    const hlsPath = `${post_id}/${id}`;
    console.log(`[JOB] Transcoding ${source_path} -> ${hlsPath}`);
    await transcodeToHls(inFile, outDir);

    console.log(`[JOB] Uploading HLS to ${HLS_BUCKET}/${hlsPath}`);
    await uploadFolder(HLS_BUCKET, hlsPath, outDir);

    const hlsPublicUrl = `${SUPABASE_URL}/storage/v1/object/public/${HLS_BUCKET}/${hlsPath}/index.m3u8`;

    await supabase
      .from("gs_posts")
      .update({
        hls_path: `${hlsPath}/index.m3u8`,
        media_url: hlsPublicUrl,
      })
      .eq("id", post_id);

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "completed",
        updated_at: nowIso(),
        last_error: null,
        error_message: null,
      })
      .eq("id", id);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "failed",
        updated_at: nowIso(),
        last_error: message.slice(0, 500),
        error_message: message.slice(0, 1000),
        attempts: attempts + 1,
      })
      .eq("id", id);

    console.error("Worker error:", err);
  } finally {
    await fsp.rm(tmpDir, { recursive: true, force: true });
  }
}

async function poll() {
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

    const { data: claimed, error: claimError } = await supabase
      .from("video_transcode_jobs")
      .update({
        status: "processing",
        locked_by: WORKER_ID,
        locked_at: nowIso(),
      })
      .eq("id", job.id)
      .eq("status", "pending")
      .select();

    if (claimError) throw claimError;
    if (!claimed || claimed.length === 0) return;

    await processJob(claimed[0]);
  } catch (err) {
    console.error("Worker error:", err);
  }
}

setInterval(poll, POLL_MS);

app.listen(PORT, () => {
  console.log(`Worker listening on ${PORT}`);
});
