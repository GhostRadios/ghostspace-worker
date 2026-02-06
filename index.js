import express from "express";
import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { pipeline } from "node:stream/promises";

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
  global: { fetch },
});

const WORKER_ID = process.env.WORKER_ID || "ghostspace-worker-new";

app.get("/", (_req, res) => {
  res.json({ ok: true, service: "ghostspace-worker" });
});

app.get("/probe", async (req, res) => {
  try {
    const rawPath = req.query.path;
    if (!rawPath) {
      return res.status(400).json({ error: "Missing ?path=raw/..." });
    }
    const url = `${SUPABASE_URL}/storage/v1/object/public/videos/${rawPath}`;
    const head = await fetch(url, { method: "HEAD" });
    res.json({
      url,
      status: head.status,
      headers: Object.fromEntries(head.headers.entries()),
    });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function downloadOnce(url, destPath) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5 * 60 * 1000);

  const res = await fetch(url, { signal: controller.signal, redirect: "follow" });
  clearTimeout(timeout);

  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  await pipeline(res.body, fs.createWriteStream(destPath));
}

async function downloadWithRetry(url, destPath, attempts = 3) {
  let lastErr;
  for (let i = 1; i <= attempts; i += 1) {
    try {
      await downloadOnce(url, destPath);
      return;
    } catch (err) {
      lastErr = err;
      console.error(`[JOB] Download failed (attempt ${i}).`, err);
      await sleep(1000 * i);
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

async function processJob(job) {
  const { id, post_id, source_path } = job;
  const publicUrl = `${SUPABASE_URL}/storage/v1/object/public/videos/${source_path}`;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gs-"));
  const sourceFile = path.join(tmpDir, "source.mp4");
  const outDir = path.join(tmpDir, "hls");

  try {
    console.log(`[JOB] Downloading ${source_path}`);
    await downloadWithRetry(publicUrl, sourceFile, 3);

    fs.mkdirSync(outDir, { recursive: true });

    console.log(`[JOB] Transcoding ${source_path}`);
    await new Promise((resolve, reject) => {
      const ff = spawn("ffmpeg", [
        "-y",
        "-i",
        sourceFile,
        "-vf",
        "scale=trunc(iw/2)*2:trunc(ih/2)*2",
        "-c:v",
        "libx264",
        "-profile:v",
        "main",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-ar",
        "48000",
        "-b:a",
        "128k",
        "-g",
        "48",
        "-sc_threshold",
        "0",
        "-hls_time",
        "2",
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        path.join(outDir, "segment_%05d.ts"),
        path.join(outDir, "index.m3u8"),
      ]);

      ff.stderr.on("data", (d) => console.log(d.toString()));
      ff.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`ffmpeg failed with code ${code}`));
      });
    });

    const hlsPath = `${post_id}/${id}`;
    await uploadFolder("videos-hls", hlsPath, outDir);

    const hlsKey = `${hlsPath}/index.m3u8`;
    const hlsPublicUrl = `${SUPABASE_URL}/storage/v1/object/public/videos-hls/${hlsKey}`;

    await supabase
      .from("posts")
      .update({
        hls_path: hlsKey,
        hls_master_path: hlsKey,
        media_url: hlsPublicUrl,
        updated_at: new Date().toISOString(),
      })
      .eq("id", post_id);

    await supabase
      .from("gs_posts")
      .update({
        hls_path: hlsKey,
        media_url: hlsPublicUrl,
        updated_at: new Date().toISOString(),
      })
      .eq("id", post_id);

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "completed",
        last_error: null,
        error_message: null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", id);

    console.log(`[JOB] Completed ${id}`);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

let isProcessing = false;

async function poll() {
  if (isProcessing) return;
  isProcessing = true;

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

    const { data: claimed } = await supabase
      .from("video_transcode_jobs")
      .update({
        status: "processing",
        locked_by: WORKER_ID,
        locked_at: new Date().toISOString(),
      })
      .eq("id", job.id)
      .eq("status", "pending")
      .select()
      .maybeSingle();

    if (!claimed) return;

    try {
      await processJob(job);
    } catch (err) {
      const attempts = (job.attempts || 0) + 1;
      const failStatus = attempts >= 3 ? "failed" : "pending";

      await supabase
        .from("video_transcode_jobs")
        .update({
          status: failStatus,
          attempts,
          last_error: String(err).slice(0, 500),
          updated_at: new Date().toISOString(),
          locked_by: null,
          locked_at: null,
        })
        .eq("id", job.id);

      console.error("Worker error:", err);
    }
  } catch (err) {
    console.error("Worker error:", err);
  } finally {
    isProcessing = false;
  }
}

setInterval(poll, 5000);

app.listen(PORT, () => {
  console.log(`Worker listening on ${PORT}`);
});
