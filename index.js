import express from "express";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import dns from "node:dns";
import { Agent, setGlobalDispatcher } from "undici";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";

dns.setDefaultResultOrder("ipv4first");
setGlobalDispatcher(new Agent({ connect: { family: 4 } }));

const app = express();
const PORT = process.env.PORT || 8080;

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const WORKER_NAME = process.env.WORKER_NAME || "ghostspace-worker-new";

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const STORAGE_PUBLIC_BASE = `${SUPABASE_URL}/storage/v1/object/public/videos`;

app.get("/", (_req, res) => {
  res.json({ ok: true, service: "ghostspace-worker" });
});

app.get("/probe", async (req, res) => {
  try {
    const sourcePath = String(req.query.path || "");
    if (!sourcePath || sourcePath.includes("..")) {
      return res.status(400).json({ error: "invalid path" });
    }

    const url = `${STORAGE_PUBLIC_BASE}/${sourcePath}`;
    const r = await fetchWithTimeout(
      url,
      { method: "HEAD", headers: { "User-Agent": "ghostspace-worker" } },
      20000
    );

    const headers = {};
    r.headers.forEach((v, k) => (headers[k] = v));

    res.json({ url, status: r.status, headers });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

async function fetchWithTimeout(url, options = {}, timeoutMs = 60000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

async function downloadWithRetry(sourcePath, destPath, attempts = 3) {
  const url = `${STORAGE_PUBLIC_BASE}/${sourcePath}`;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    const res = await fetchWithTimeout(
      url,
      { headers: { "User-Agent": "ghostspace-worker", Accept: "*/*" } },
      90000
    );

    if (res.ok && res.body) {
      await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(destPath));
      return;
    }

    const text = await safeReadText(res);
    console.error(
      `[DOWNLOAD] attempt ${attempt} failed: HTTP ${res.status} ${text}`
    );

    if (attempt === attempts) {
      throw new Error(`HTTP ${res.status} ${text}`);
    }

    await sleep(1500 * attempt);
  }
}

async function safeReadText(res) {
  try {
    const txt = await res.text();
    return (txt || "").slice(0, 300);
  } catch {
    return "";
  }
}

async function uploadFolder(bucket, prefix, folderPath) {
  const files = fs.readdirSync(folderPath);
  for (const file of files) {
    const full = path.join(folderPath, file);
    const stat = fs.statSync(full);

    if (stat.isDirectory()) {
      await uploadFolder(bucket, `${prefix}/${file}`, full);
      continue;
    }

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

async function failJob(id, message) {
  const msg = (message || "unknown").slice(0, 500);

  await supabase
    .from("video_transcode_jobs")
    .update({
      status: "failed",
      last_error: msg,
      error_message: msg,
      updated_at: new Date().toISOString(),
    })
    .eq("id", id);
}

async function processJob(job) {
  const { id, post_id, source_path } = job;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gs-"));
  const sourceFile = path.join(tmpDir, "source.mp4");
  const outDir = path.join(tmpDir, "hls");
  fs.mkdirSync(outDir, { recursive: true });

  const hlsPrefix = `${post_id}/${id}`;
  const hlsIndex = `${hlsPrefix}/index.m3u8`;
  const hlsPublicUrl = `${SUPABASE_URL}/storage/v1/object/public/videos-hls/${hlsIndex}`;

  try {
    console.log(`[JOB] Downloading ${source_path}`);
    await downloadWithRetry(source_path, sourceFile);

    console.log(`[JOB] Transcoding ${source_path} -> ${hlsPrefix}`);
    await new Promise((resolve, reject) => {
      const ff = spawn("ffmpeg", [
        "-y",
        "-i",
        sourceFile,
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

    console.log(`[JOB] Uploading HLS to videos-hls/${hlsPrefix}`);
    await uploadFolder("videos-hls", hlsPrefix, outDir);

    await supabase.from("posts").update({ hls_path: hlsIndex }).eq("id", post_id);

    await supabase
      .from("gs_posts")
      .update({ hls_path: hlsIndex, media_url: hlsPublicUrl })
      .eq("id", post_id);

    await supabase
      .from("video_transcode_jobs")
      .update({ status: "completed", updated_at: new Date().toISOString() })
      .eq("id", id);
  } catch (err) {
    console.error("Worker error:", err);
    await failJob(id, String(err));
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

let busy = false;

async function poll() {
  if (busy) return;

  busy = true;
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

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "processing",
        locked_by: WORKER_NAME,
        locked_at: new Date().toISOString(),
      })
      .eq("id", job.id);

    await processJob(job);
  } catch (err) {
    console.error("Worker error:", err);
  } finally {
    busy = false;
  }
}

setInterval(poll, 5000);

app.listen(PORT, () => {
  console.log(`Worker listening on ${PORT}`);
});
