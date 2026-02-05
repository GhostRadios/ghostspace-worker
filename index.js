import express from "express";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

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

app.get("/", (_req, res) => {
  res.json({ ok: true, service: "ghostspace-worker" });
});

async function uploadFolder(bucket, prefix, folderPath) {
  const files = fs.readdirSync(folderPath);
  for (const file of files) {
    const full = path.join(folderPath, file);
    const stat = fs.statSync(full);
    if (stat.isDirectory()) {
      await uploadFolder(bucket, `${prefix}/${file}`, full);
    } else {
      const content = fs.readFileSync(full);
      await supabase.storage.from(bucket).upload(`${prefix}/${file}`, content, {
        contentType: file.endsWith(".m3u8")
          ? "application/vnd.apple.mpegurl"
          : "video/MP2T",
        upsert: true,
      });
    }
  }
}

async function processJob(job) {
  const { id, post_id, source_path } = job;

  // usa URL pública direta (bucket videos é público)
  const publicUrl = `${SUPABASE_URL}/storage/v1/object/public/videos/${source_path}`;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gs-"));
  const outDir = path.join(tmpDir, "hls");
  fs.mkdirSync(outDir, { recursive: true });

  const hlsPath = `${post_id}/${id}`;

  console.log(`[JOB] Transcoding ${source_path} -> ${hlsPath}`);
  await new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", [
      "-y",
      "-reconnect", "1",
      "-reconnect_streamed", "1",
      "-reconnect_delay_max", "5",
      "-i", publicUrl,
      "-preset", "veryfast",
      "-g", "48",
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

  console.log(`[JOB] Uploading HLS to videos-hls/${hlsPath}`);
  await uploadFolder("videos-hls", hlsPath, outDir);

  await supabase
    .from("posts")
    .update({ hls_path: `${hlsPath}/index.m3u8` })
    .eq("id", post_id);

  await supabase
    .from("video_transcode_jobs")
    .update({ status: "completed", updated_at: new Date().toISOString() })
    .eq("id", id);
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

    await supabase
      .from("video_transcode_jobs")
      .update({
        status: "processing",
        locked_by: "ghostspace-worker-new",
        locked_at: new Date().toISOString(),
      })
      .eq("id", job.id);

    await processJob(job);
  } catch (err) {
    console.error("Worker error:", err);
  }
}

setInterval(poll, 5000);

app.listen(PORT, () => {
  console.log(`Worker listening on ${PORT}`);
});
