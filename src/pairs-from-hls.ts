/**
 * pairs-from-hls.ts
 *
 * For each detection timestamp ± buffer:
 *   1) find overlapping HLS playlists (GraphQL)
 *   2) compute 10s segment URLs
 *   3) EITHER:
 *        --via api    : POST to a concat REST API that returns a FLAC
 *        --via ffmpeg : download .ts locally (concurrent), then concat to FLAC via ffmpeg
 *   4) optionally upload FLAC to S3 (HTTPS URL returned, multipart)
 *   5) append a CSV row with detection fields + windowStart/windowEnd + playlistTimestamps + chunkFiles + flacPath
 *
 * Usage examples:
 *   npx ts-node pairs-from-hls.ts detections.json --via ffmpeg --toS3 --bucket acoustic-sandbox --prefix clap-model/ --region us-east-2 --skipIfExists
 *   npx ts-node pairs-from-hls.ts detections.json --via api --api https://your.concat.endpoint/concat --toS3 --bucket acoustic-sandbox --prefix clap-model/
 *
 * Requires: Node 18+, ffmpeg (if --via ffmpeg), @aws-sdk/client-s3, @aws-sdk/lib-storage, dotenv
 */

import "dotenv/config";
import { promises as fs } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { pipeline } from "node:stream/promises";
import { createWriteStream } from "node:fs";
import { execFile } from "node:child_process";
import {
  S3Client,
  HeadObjectCommand,
  GetBucketLocationCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

/* ---------------------------------- CLI ---------------------------------- */

const jsonPath = process.argv[2];
if (!jsonPath || jsonPath.startsWith("--")) {
  console.error(
    "Usage: npx ts-node hls-csv-to-api.ts <detections.json> " +
      "[--buffer 30] [--via api|ffmpeg] [--api <URL>] [--toS3] " +
      "[--bucket <name>] [--prefix <path/>] [--region <aws-region>] [--skipIfExists]"
  );
  process.exit(1);
}

const took = (ms: number) => `${(ms / 1000).toFixed(2)}s`;

let BUFFER_SEC = 30;
let VIA: "api" | "ffmpeg" = "api";
let API_URL = "";
let TO_S3 = false;
let BUCKET: string | null = null;
let PREFIX = "";
let AWS_REGION = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-2";
let SKIP_IF_EXISTS = false;
let KEEP_LOCAL = false;
let APPEND_CSV = true;
let RESUME_CSV = false;
let CONCURRENCY = 1;
let FLAC_LEVEL = 3;
let ENCODE_CONCURRENCY = 4; // concurrent ffmpeg workers
let UPLOAD_CONCURRENCY = 3; // concurrent S3 multipart uploads
let DOWNLOAD_CONCURRENCY = 8; // concurrent .ts downloads per detection
let PRELIST_S3 = true; // build a set of existing keys under --prefix to skip w/o HEAD
let PREFER_PLAYLIST_TS = true; // if detection has playlistTimestamp, try that path first

for (let i = 3; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === "--buffer") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v) && v >= 0) BUFFER_SEC = v;
  } else if (a === "--via") {
    const v = String(process.argv[i + 1]).toLowerCase(); i++;
    if (v === "api" || v === "ffmpeg") VIA = v as any;
  } else if (a === "--api") {
    API_URL = process.argv[i + 1]; i++;
  } else if (a === "--toS3") {
    TO_S3 = true;
  } else if (a === "--bucket") {
    BUCKET = process.argv[i + 1]; i++;
  } else if (a === "--prefix") {
    PREFIX = process.argv[i + 1] || ""; i++;
  } else if (a === "--region") {
    AWS_REGION = process.argv[i + 1]; i++;
  } else if (a === "--skipIfExists") {
    SKIP_IF_EXISTS = true;
  } else if (a === "--s3Bucket") {
    console.error("Flag --s3Bucket is deprecated. Use --bucket <name> and --prefix <path/> instead.");
    process.exit(1);
  } else if (a === "--keepLocal") {
    KEEP_LOCAL = true;
  } else if (a === "--appendCsv") {
    const v = String(process.argv[i + 1] ?? "true").toLowerCase(); i++;
    APPEND_CSV = v !== "false";
  } else if (a === "--resumeCsv") {
    RESUME_CSV = true;
  } else if (a === "--concurrency") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v) && v >= 1 && v <= 64) CONCURRENCY = v;
  } else if (a === "--flacLevel") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v)) FLAC_LEVEL = v;
  } else if (a === "--encodeConcurrency") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v) && v >= 1) ENCODE_CONCURRENCY = v;
  } else if (a === "--uploadConcurrency") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v) && v >= 1) UPLOAD_CONCURRENCY = v;
  } else if (a === "--downloadConcurrency") {
    const v = Number(process.argv[i + 1]); i++;
    if (Number.isFinite(v) && v >= 1) DOWNLOAD_CONCURRENCY = v;
  } else if (a === "--noPrelistS3") {
    PRELIST_S3 = false;
  } else if (a === "--noPreferPlaylistTs") {
    PREFER_PLAYLIST_TS = false;
  }
}
if (PREFIX && !PREFIX.endsWith("/")) PREFIX += "/";

/* --------------------------- S3 setup & helpers --------------------------- */

let s3 = new S3Client({ region: AWS_REGION });

function normalizeLocationConstraint(v?: string | null): string {
  if (!v) return "us-east-1";
  if (v === "EU") return "eu-west-1";
  return v;
}

let _resolvedBucketRegion: string | null = null;

async function ensureS3ForBucket(bucket: string) {
  if (_resolvedBucketRegion) return;
  try {
    const out = await s3.send(new GetBucketLocationCommand({ Bucket: bucket }));
    const trueRegion = normalizeLocationConstraint(out.LocationConstraint as string | undefined);
    if (trueRegion && trueRegion !== AWS_REGION) {
      console.log(`[s3] Bucket ${bucket} is in ${trueRegion}; reinitializing S3 client (was ${AWS_REGION})`);
      AWS_REGION = trueRegion;
      s3 = new S3Client({ region: AWS_REGION });
    } else {
      console.log(`[s3] Using region ${AWS_REGION} for bucket ${bucket}`);
    }
    _resolvedBucketRegion = AWS_REGION;
  } catch (err: any) {
    console.warn(`[s3] GetBucketLocation failed (${err?.name || "Error"}). Proceeding with region ${AWS_REGION}.`);
  }
}

function s3HttpsUrl(bucket: string, key: string): string {
  const base =
    AWS_REGION === "us-east-1"
      ? `https://${bucket}.s3.amazonaws.com`
      : `https://${bucket}.s3.${AWS_REGION}.amazonaws.com`;
  return `${base}/${key}`;
}

async function s3Head(bucket: string, key: string): Promise<boolean> {
  await ensureS3ForBucket(bucket);
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (err: any) {
    if (err?.$metadata?.httpStatusCode === 404) return false;
    throw err;
  }
}

async function s3UploadFile(
  bucket: string,
  key: string,
  filePath: string,
  opts?: { publicRead?: boolean }
): Promise<string> {
  await ensureS3ForBucket(bucket);
  const Body = await fs.readFile(filePath);

  const baseParams: any = { Bucket: bucket, Key: key, Body, ContentType: "audio/flac" };

  if (opts?.publicRead) {
    try {
      await new Upload({
        client: s3,
        params: { ...baseParams, ACL: "public-read" },
        queueSize: 6,
        partSize: 8 * 1024 * 1024,
        leavePartsOnError: false,
      }).done();
      return s3HttpsUrl(bucket, key);
    } catch (err: any) {
      const msg = String(err?.message || "");
      if (/Acl|AccessDenied|PutObjectAcl/i.test(msg)) {
        console.warn(`[s3] ACL not permitted; retrying upload WITHOUT ACL for s3://${bucket}/${key}`);
      } else {
        throw err;
      }
    }
  }

  await new Upload({
    client: s3,
    params: baseParams,
    queueSize: 6,
    partSize: 8 * 1024 * 1024,
    leavePartsOnError: false,
  }).done();

  return s3HttpsUrl(bucket, key);
}

/* --------------------------- URL builders (HLS) --------------------------- */

function buildHlsBase(feedNodeName: string, playlistId: string): string {
  return `https://audio-orcasound-net.s3.amazonaws.com/${feedNodeName}/hls/${playlistId}`;
}
function buildTsUrl(feedNodeName: string, playlistId: string, index: number): string {
  const n = String(index).padStart(3, "0");
  return `${buildHlsBase(feedNodeName, playlistId)}/live${n}.ts`;
}

/* -------------------------------- GraphQL -------------------------------- */

const endpoint = process.env.ORCASOUND_GQL || "https://live.orcasound.net/graphql";

type GqlResult<T> = { data?: T; errors?: Array<{ message: string }> };

async function gqlRequest<T>(query: string): Promise<T> {
  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json", connection: "keep-alive" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`GraphQL HTTP ${res.status}${txt ? `: ${txt.slice(0, 120)}` : ""}`);
  }
  const json = (await res.json()) as GqlResult<T>;
  if (json.errors?.length) throw new Error(`GraphQL error: ${json.errors.map((e) => e.message).join("; ")}`);
  if (!json.data) throw new Error("GraphQL: missing data");
  return json.data;
}

type FeedSegment = {
  startTime: string;
  endTime: string;
  duration: number;
  playlistTimestamp: string;
  feedId: string;
};

type FeedStream = {
  startTime: string;
  endTime: string | null;
  duration: number | null;
  playlistTimestamp: string;
  feedId: string;
};

type FeedSegmentsResponse = { feedSegments: { results: FeedSegment[] } };
type FeedStreamsResponse = { feedStreams: { results: FeedStream[] } };

/** Overlap: segment.startTime <= windowEnd AND segment.endTime >= windowStart */
const GET_SEGMENTS = (feedId: string, windowStart: string, windowEnd: string) => `
{
  feedSegments(
    feedId: "${feedId}",
    filter: {
      startTime: { lessThanOrEqual: "${windowEnd}" },
      endTime:   { greaterThanOrEqual: "${windowStart}" }
    },
    sort: { field: START_TIME, order: ASC }
  ) {
    results {
      startTime
      endTime
      duration
      playlistTimestamp
      feedId
    }
  }
}
`;

const GET_STREAMS = (feedId: string, playlistTimestamp: string) => `
{
  feedStreams(
    feedId: "${feedId}",
    filter: { playlistTimestamp: { eq: "${playlistTimestamp}" } }
  ) {
    results {
      startTime
      endTime
      duration
      playlistTimestamp
      feedId
    }
  }
}
`;

// Caches
const streamCache = new Map<string, FeedStream | null>(); // key: `${feedId}|${playlistTimestamp}`
const segmentsCache = new Map<string, FeedSegment[]>();    // key: `${feedId}|${startIso}|${endIso}`

async function fetchSegmentsCached(feedId: string, startIso: string, endIso: string) {
  const key = `${feedId}|${startIso}|${endIso}`;
  if (segmentsCache.has(key)) return segmentsCache.get(key)!;
  const data = await gqlRequest<FeedSegmentsResponse>(GET_SEGMENTS(feedId, startIso, endIso));
  const v = data.feedSegments.results ?? [];
  segmentsCache.set(key, v);
  return v;
}

async function fetchStreamCached(feedId: string, playlistTimestamp: string) {
  const key = `${feedId}|${playlistTimestamp}`;
  if (streamCache.has(key)) return streamCache.get(key)!;
  const data = await gqlRequest<FeedStreamsResponse>(GET_STREAMS(feedId, playlistTimestamp));
  const v = data.feedStreams.results?.[0] ?? null;
  streamCache.set(key, v);
  return v;
}

/* -------------------------------- Helpers -------------------------------- */

function csvEscape(val: any): string {
  if (val === null || val === undefined) return "";
  const s = String(val);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function flattenDetection(det: any): Record<string, any> {
  const flat: Record<string, any> = { ...det };
  if (det.feed && typeof det.feed === "object") {
    flat["feed.id"] = det.feed.id ?? "";
    flat["feed.name"] = det.feed.name ?? "";
    flat["feed.nodeName"] = det.feed.nodeName ?? "";
    delete flat.feed;
  }
  if (det.candidate && typeof det.candidate === "object") {
    flat["candidate.id"] = det.candidate.id ?? "";
    flat["candidate.feedId"] = det.candidate.feedId ?? "";
    delete flat.candidate;
  }
  return flat;
}

function toUtcDate(x: string | Date): Date {
  let s = typeof x === "string" ? x : x.toISOString();
  if (/T\d{2}-\d{2}-\d{2}/.test(s)) s = s.replace(/T(\d{2})-(\d{2})-(\d{2})/, "T$1:$2:$3");
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) throw new Error(`Invalid date: ${s}`);
  return d;
}
const iso = (d: Date) => d.toISOString();
const HLS_BUCKET_SEC = 10;
const clamp = (a: number, min: number, max: number) => Math.max(min, Math.min(max, a));
const offsetSeconds = (base: Date, at: Date) => (at.getTime() - base.getTime()) / 1000;
const safeIsoForFile = (d: Date) => d.toISOString().replace(/:/g, "-").replace(/\//g, "-");

/* ----------------------- Segment resolution (cached) ---------------------- */

async function segmentsForWindow(
  feedId: string,
  winStart: Date,
  winEnd: Date,
  feedNodeName: string,
  fallbackPlaylistTimestamp?: string
): Promise<{ playlistTimestamps: string[]; chunkFiles: string[] }> {
  // Fast path: if detection provided playlistTimestamp, attempt to build directly
  if (PREFER_PLAYLIST_TS && fallbackPlaylistTimestamp) {
    const pt = String(fallbackPlaylistTimestamp);
    const stream = await fetchStreamCached(feedId, pt);
    if (stream && stream.startTime) {
      const st = toUtcDate(stream.startTime);
      const et = stream.endTime ? toUtcDate(stream.endTime) : null;
      const pStart = new Date(Math.max(winStart.getTime(), st.getTime()));
      const pEnd = new Date(Math.min(winEnd.getTime(), et ? et.getTime() : winEnd.getTime()));
      if (pEnd > pStart) {
        const startOff = clamp(offsetSeconds(st, pStart), 0, Number.MAX_SAFE_INTEGER);
        const endOff   = clamp(offsetSeconds(st, pEnd),   0, Number.MAX_SAFE_INTEGER);
        const startIdx = Math.floor(startOff / HLS_BUCKET_SEC) + 1;
        const endIdx   = Math.ceil(endOff / HLS_BUCKET_SEC);
        if (endIdx >= startIdx) {
          const urls: string[] = [];
          for (let i = startIdx; i <= endIdx; i++) urls.push(buildTsUrl(feedNodeName, pt, i));
          if (urls.length) return { playlistTimestamps: [pt], chunkFiles: urls };
        }
      }
    }
  }

  // Otherwise query overlapping segments
  let segs: FeedSegment[] = [];
  try { segs = await fetchSegmentsCached(feedId, iso(winStart), iso(winEnd)); }
  catch { segs = []; }

  if (segs.length) {
    const byPlaylist = new Map<string, FeedSegment[]>();
    for (const s of segs) {
      if (!byPlaylist.has(s.playlistTimestamp)) byPlaylist.set(s.playlistTimestamp, []);
      byPlaylist.get(s.playlistTimestamp)!.push(s);
    }

    const playlistKeys: string[] = [];
    const allPaths: string[] = [];

    for (const [pt] of byPlaylist.entries()) {
      const stream = await fetchStreamCached(feedId, pt);
      if (!stream || !stream.startTime) continue;

      const st = toUtcDate(stream.startTime);
      const et = stream.endTime ? toUtcDate(stream.endTime) : null;

      const pStart = new Date(Math.max(winStart.getTime(), st.getTime()));
      const pEnd = new Date(Math.min(winEnd.getTime(), et ? et.getTime() : winEnd.getTime()));
      if (pEnd <= pStart) continue;

      const startOff = clamp(offsetSeconds(st, pStart), 0, Number.MAX_SAFE_INTEGER);
      const endOff   = clamp(offsetSeconds(st, pEnd),   0, Number.MAX_SAFE_INTEGER);
      const startIdx = Math.floor(startOff / HLS_BUCKET_SEC) + 1;
      const endIdx   = Math.ceil(endOff / HLS_BUCKET_SEC);
      if (endIdx < startIdx) continue;

      const urls: string[] = [];
      for (let i = startIdx; i <= endIdx; i++) urls.push(buildTsUrl(feedNodeName, pt, i));
      if (urls.length) { playlistKeys.push(pt); allPaths.push(...urls); }
    }

    if (allPaths.length) return { playlistTimestamps: playlistKeys, chunkFiles: allPaths };
  }

  // Final fallback: try the provided playlistTimestamp again
  if (fallbackPlaylistTimestamp) {
    const pt = String(fallbackPlaylistTimestamp);
    const stream = await fetchStreamCached(feedId, pt);
    if (stream && stream.startTime) {
      const st = toUtcDate(stream.startTime);
      const et = stream.endTime ? toUtcDate(stream.endTime) : null;

      const pStart = new Date(Math.max(winStart.getTime(), st.getTime()));
      const pEnd = new Date(Math.min(winEnd.getTime(), et ? et.getTime() : winEnd.getTime()));
      if (pEnd > pStart) {
        const startOff = clamp(offsetSeconds(st, pStart), 0, Number.MAX_SAFE_INTEGER);
        const endOff   = clamp(offsetSeconds(st, pEnd),   0, Number.MAX_SAFE_INTEGER);
        const startIdx = Math.floor(startOff / HLS_BUCKET_SEC) + 1;
        const endIdx   = Math.ceil(endOff / HLS_BUCKET_SEC);
        if (endIdx >= startIdx) {
          const urls: string[] = [];
          for (let i = startIdx; i <= endIdx; i++) urls.push(buildTsUrl(feedNodeName, pt, i));
          if (urls.length) return { playlistTimestamps: [pt], chunkFiles: urls };
        }
      }
    }
  }

  return { playlistTimestamps: [], chunkFiles: [] };
}

/* -------- Concat via API (writes to local outPath; caller handles cleanup) -------- */

async function concatFlacViaApi(apiUrl: string, segmentUrls: string[], outPath: string): Promise<string> {
  const res = await fetch(apiUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ segments: segmentUrls }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`);
  }
  const buf = Buffer.from(await res.arrayBuffer());
  await fs.mkdir(dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, buf);
  return outPath;
}

/* ----------------------- Download + concat via ffmpeg --------------------- */

async function httpDownload(url: string, dest: string): Promise<void> {
  const r = await fetch(url, { redirect: "follow", headers: { connection: "keep-alive" } });
  if (!r.ok || !r.body) throw new Error(`HTTP ${r.status} for ${url}`);
  await fs.mkdir(dirname(dest), { recursive: true });
  const out = createWriteStream(dest);
  await pipeline(r.body as any, out);
}

async function downloadMany(urls: string[], dir: string, concurrency: number): Promise<string[]> {
  const outFiles = new Array<string>(urls.length);
  let i = 0;
  const executing: Promise<void>[] = [];

  const worker = async (idx: number) => {
    const u = urls[idx];
    const fname = `${String(idx + 1).padStart(3, "0")}.ts`;
    const dest = join(dir, fname);
    const maxRetry = 3;
    for (let a = 1; a <= maxRetry; a++) {
      try {
        await httpDownload(u, dest);
        outFiles[idx] = dest;
        return;
      } catch (err) {
        if (a === maxRetry) throw err;
        await new Promise((r) => setTimeout(r, 250 * a));
      }
    }
  };

  const spawn = async (): Promise<void> => {
    if (i >= urls.length) return Promise.allSettled(executing).then(() => undefined);
    const idx = i++;
    const p = worker(idx).finally(() => {
      const j = executing.indexOf(p);
      if (j >= 0) executing.splice(j, 1);
    });
    executing.push(p);
    if (executing.length >= concurrency) await Promise.race(executing);
    return spawn();
  };

  await spawn();
  return outFiles.filter(Boolean);
}

// Build list.txt in its own temp dir; never under OUT_DIR
async function concatLocalTsToFlac(localTsFiles: string[], outPath: string): Promise<void> {
  const listDir = await fs.mkdtemp(join(tmpdir(), "hls-list-"));
  const listPath = join(listDir, "list.txt");
  try {
    const listTxt = localTsFiles.map((p) => `file '${p.replace(/'/g, "'\\''")}'`).join("\n");
    await fs.writeFile(listPath, listTxt, "utf8");
    await fs.mkdir(dirname(outPath), { recursive: true });
    await new Promise<void>((resolve, reject) => {
      const args = [
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", listPath,
        "-vn",
        "-acodec", "flac",
        "-compression_level", String(FLAC_LEVEL),
        outPath,
      ];
      execFile("ffmpeg", args, (err, _stdout, stderr) => {
        if (err) reject(new Error(`ffmpeg failed: ${stderr?.toString() ?? err?.message}`));
        else resolve();
      });
    });
  } finally {
    await fs.rm(listDir, { recursive: true, force: true }).catch(() => {});
  }
}

/* ---------------------------- Concurrency utils --------------------------- */

async function asyncPool<T>(
  concurrency: number,
  items: T[],
  worker: (item: T, index: number) => Promise<void>
) {
  let i = 0;
  const executing: Promise<void>[] = [];
  const enqueue = async (): Promise<void | PromiseSettledResult<void>[]> => {
    if (i >= items.length) return Promise.allSettled(executing);
    const idx = i++;
    const p = worker(items[idx], idx).finally(() => {
      const j = executing.indexOf(p);
      if (j >= 0) executing.splice(j, 1);
    });
    executing.push(p);
    if (executing.length >= concurrency) await Promise.race(executing);
    return enqueue();
  };
  await enqueue();
}

function makeCsvWriter(csvStream: import("node:fs").WriteStream) {
  let chain = Promise.resolve();
  return (line: string) =>
    (chain = chain.then(
      () =>
        new Promise<void>((resolve) => {
          csvStream.write(line, () => resolve());
        })
    ));
}

function makeSemaphore(n: number) {
  let avail = n,
    queue: (() => void)[] = [];
  return {
    async acquire() {
      if (avail > 0) {
        avail--;
        return;
      }
      await new Promise<void>((res) => queue.push(res));
    },
    release() {
      const next = queue.shift();
      if (next) next();
      else avail++;
    },
  };
}

async function s3ListAllKeys(bucket: string, prefix: string): Promise<Set<string>> {
  await ensureS3ForBucket(bucket);
  const found = new Set<string>();
  let token: string | undefined = undefined;
  let pages = 0, total = 0;
  const t0 = Date.now();

  do {
    const out: ListObjectsV2CommandOutput = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        ContinuationToken: token,
      })
    );
    pages++;
    for (const o of out.Contents ?? []) {
      if (o.Key) { found.add(o.Key); total++; }
    }
    token = out.IsTruncated ? out.NextContinuationToken : undefined;
    if (pages % 10 === 0) {
      console.log(`[s3] prelist page ${pages} | keys=${total} | ${took(Date.now() - t0)}`);
    }
  } while (token);

  console.log(`[s3] prelist complete: ${total} keys under s3://${bucket}/${prefix} in ${took(Date.now() - t0)}`);
  return found;
}

/* --------------------------------- Main ---------------------------------- */

async function main() {
  const raw = await fs.readFile(jsonPath, "utf8");
  const parsed = JSON.parse(raw);
  const detections: any[] = parsed.detections ?? [];

  const OUT_DIR = join(process.cwd(), "hls_api_outputs");
  await fs.mkdir(OUT_DIR, { recursive: true });
  const CSV_BASENAME = `detections_hls_segments_with_flac_${VIA}.csv`;
  const CSV_OUT = join(OUT_DIR, CSV_BASENAME);

  console.log(
    `Loaded ${detections.length} detections from ${jsonPath} | buffer=${BUFFER_SEC}s | via=${VIA}` +
      (TO_S3 && BUCKET ? ` | upload→ ${s3HttpsUrl(BUCKET, PREFIX || "")}` : "")
  );

  if (VIA === "api" && !API_URL) {
    console.error("Missing --api <URL>.");
    process.exit(1);
  }
  if (TO_S3 && !BUCKET) {
    console.error("When --toS3 is set, you must provide --bucket <name> (and optionally --prefix <path/>)");
    process.exit(1);
  }

  // CSV header/stream
  const flattened = detections.map(flattenDetection);
  const keySet = new Set<string>();
  for (const f of flattened) Object.keys(f).forEach((k) => keySet.add(k));
  const baseColumns = Array.from(keySet).sort();

  // Add our fixed trailing columns
  const trailing = [
    "windowStart",
    "windowEnd",
    "playlistTimestamps",
    "chunkFiles",
    "flacPath",
    "status",
    "errorStage",
    "errorMessage",
  ];
  const columns = [...baseColumns, ...trailing];

  let csvExists = false;
  let csvNonEmpty = false;
  try {
    const st = await fs.stat(CSV_OUT);
    csvExists = true;
    csvNonEmpty = st.size > 0;
  } catch {}

  // Optional: de-dup on resume by reading existing flacPath or a detection key
  const alreadyKeys = new Set<string>();
  if (RESUME_CSV && csvNonEmpty) {
    const content = await fs.readFile(CSV_OUT, "utf8");
    const lines = content.split(/\r?\n/);
    const header = lines.shift() || "";
    const cols = header.split(",");
    const idxFlac = cols.indexOf("flacPath");
    const idxDetId = cols.indexOf("id");
    for (const line of lines) {
      if (!line.trim()) continue;
      const parts = line.split(",");
      const k = idxFlac >= 0 && parts[idxFlac] ? parts[idxFlac] : idxDetId >= 0 ? parts[idxDetId] : "";
      if (k) alreadyKeys.add(k);
    }
  }

  const csvStream = createWriteStream(CSV_OUT, { flags: APPEND_CSV && csvExists ? "a" : "w" });
  if (!(APPEND_CSV && csvExists && csvNonEmpty)) {
    csvStream.write(columns.map(csvEscape).join(",") + "\n");
  }

  // Prelist S3 keys (optional) for instant early-skip (but AFTER we compute segments)
  let prelistedKeys: Set<string> | null = null;
  if (PRELIST_S3 && TO_S3 && BUCKET) {
    prelistedKeys = await s3ListAllKeys(BUCKET, PREFIX);
  }

  // Graceful shutdown on Ctrl+C
  let shuttingDown = false;
  process.on("SIGINT", async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log("\nSIGINT received, closing CSV...");
    csvStream.end();
    csvStream.on("close", () => process.exit(0));
  });

  const writeCsvLine = makeCsvWriter(csvStream);

  const encSem = makeSemaphore(ENCODE_CONCURRENCY);
  const upSem = makeSemaphore(UPLOAD_CONCURRENCY);

  async function processDetection(det: any, zeroIdx: number) {
    const idx = zeroIdx + 1;

    // Init per-row fields so we always write a row
    let windowStart = "";
    let windowEnd = "";
    let playlistTimestamps: string[] = [];
    let chunkFiles: string[] = [];
    let flacPath = "";
    let status = "";
    let errorStage = "";
    let errorMessage = "";

    const t0 = Date.now();
    try {
      const ts: string | null = det.timestamp ?? null;
      const feedId: string | null = det.feed?.id ?? det.feedId ?? null;
      const feedNodeName: string | null = det.feed?.nodeName ?? null;
      const fallbackPt: string | number | null = det.playlistTimestamp != null ? det.playlistTimestamp : null;

      if (!ts || !feedId || !feedNodeName) {
        status = "invalid_input";
        errorStage = "validate";
        errorMessage = "Missing timestamp, feedId, or feed.nodeName";
        console.warn(`[${idx}] skipped (invalid input)`);
      } else {
        const center = toUtcDate(ts);
        const start = new Date(center.getTime() - BUFFER_SEC * 1000);
        const end = new Date(center.getTime() + BUFFER_SEC * 1000);
        windowStart = iso(start);
        windowEnd = iso(end);

        const outBase = `${feedNodeName}_${safeIsoForFile(start)}_${safeIsoForFile(end)}.flac`;
        const s3Key = PREFIX + outBase;

        // If we're resuming and the exact S3 URL already appeared in prior CSV, mark and short-circuit later
        const alreadyUrl = BUCKET ? s3HttpsUrl(BUCKET, s3Key) : "";

        // 1) ALWAYS compute segments first (so CSV has chunkFiles even if we skip upload)
        const tg0 = Date.now();
        let segErr: any = null;
        try {
          const segRes = await segmentsForWindow(
            feedId, start, end, feedNodeName,
            fallbackPt != null ? String(fallbackPt) : undefined
          );
          playlistTimestamps = segRes.playlistTimestamps;
          chunkFiles = segRes.chunkFiles;
        } catch (e: any) {
          segErr = e;
          playlistTimestamps = [];
          chunkFiles = [];
        }
        const tg = Date.now() - tg0;
        console.log(`[${idx}/${detections.length}] gql ${took(tg)} segs=${chunkFiles.length}${segErr ? " (SEGMENTS ERROR)" : ""}`);

        // Resume CSV de-dup
        if (RESUME_CSV && alreadyUrl && alreadyKeys.has(alreadyUrl)) {
          status = "resume_skip_in_csv";
          console.log(`[${idx}/${detections.length}] RESUME skip (already in CSV): ${alreadyUrl}`);
        } else if (chunkFiles.length === 0) {
          status = segErr ? "error_no_segments" : "no_segments";
          if (segErr) {
            errorStage = "segments";
            errorMessage = String(segErr?.message || segErr);
          }
          console.log(`[${idx}/${detections.length}] ${feedId} ${ts} → 0 segments`);
        } else {
          // 2) If requested, check for existing S3 object NOW (after we computed segments)
          if (SKIP_IF_EXISTS && TO_S3 && BUCKET) {
            let exists = false;
            let existedVia = "head";
            if (prelistedKeys?.has(s3Key)) {
              exists = true;
              existedVia = "prelist";
            } else {
              const th0 = Date.now();
              exists = await s3Head(BUCKET, s3Key);
              const th = Date.now() - th0;
              console.log(`[${idx}/${detections.length}] head ${took(th)} ${exists ? "(found)" : "(not found)"}`);
            }
            if (exists) {
              status = `skipped_exists_${existedVia}`;
              flacPath = s3HttpsUrl(BUCKET, s3Key);
              console.log(`[${idx}/${detections.length}] EARLY SKIP (${existedVia}) ${flacPath}`);
            }
          }

          // 3) If we still need to build/upload a FLAC
          if (!status.startsWith("skipped_exists")) {
            const useTempOut = !KEEP_LOCAL && TO_S3;
            const LOCAL_DIR = useTempOut ? await fs.mkdtemp(join(tmpdir(), "hls-out-")) : OUT_DIR;
            const localOut = join(LOCAL_DIR, outBase);

            // Download + concat
            let segDir = "";
            try {
              const td0 = Date.now();
              segDir = await fs.mkdtemp(join(tmpdir(), "hls-dl-"));
              let localTsFiles: string[] = [];
              try {
                localTsFiles = await downloadMany(chunkFiles, segDir, DOWNLOAD_CONCURRENCY);
              } catch (e: any) {
                status = "error_download";
                errorStage = "download";
                errorMessage = String(e?.message || e);
                throw e;
              }
              const td = Date.now() - td0;
              console.log(`[${idx}/${detections.length}] dl ${took(td)} files=${localTsFiles.length}`);

              try {
                if (VIA === "api" && API_URL) {
                  const ta0 = Date.now();
                  await concatFlacViaApi(API_URL, chunkFiles, localOut);
                  console.log(`[${idx}/${detections.length}] api concat ${took(Date.now() - ta0)}`);
                } else {
                  await encSem.acquire();
                  const te0 = Date.now();
                  try {
                    await concatLocalTsToFlac(localTsFiles, localOut);
                  } finally {
                    encSem.release();
                  }
                  console.log(`[${idx}/${detections.length}] enc ${took(Date.now() - te0)} level=${FLAC_LEVEL}`);
                }
              } catch (e: any) {
                status = status || "error_encode";
                errorStage = errorStage || "encode";
                errorMessage = errorMessage || String(e?.message || e);
                // Clean partial flac
                await fs.unlink(localOut).catch(() => {});
                throw e;
              } finally {
                // cleanup .ts
                await Promise.all(localTsFiles.map((f) => fs.unlink(f).catch(() => {})));
                if (segDir) await fs.rm(segDir, { recursive: true, force: true }).catch(() => {});
              }

              // Upload (or keep local)
              const tu0 = Date.now();
              if (TO_S3 && BUCKET) {
                await upSem.acquire();
                try {
                  flacPath = await s3UploadFile(BUCKET, s3Key, localOut);
                } catch (e: any) {
                  status = "error_upload";
                  errorStage = "upload";
                  errorMessage = String(e?.message || e);
                  throw e;
                } finally {
                  upSem.release();
                }
                console.log(
                  `[${idx}/${detections.length}] up ${took(Date.now() - tu0)} → ${flacPath}`
                );
                if (!KEEP_LOCAL) {
                  await fs.unlink(localOut).catch(() => {});
                  if (useTempOut) await fs.rm(LOCAL_DIR, { recursive: true, force: true }).catch(() => {});
                }
                status = status || "uploaded";
              } else {
                flacPath = localOut;
                status = status || "saved_local";
                console.log(`[${idx}/${detections.length}] saved local`);
              }
            } catch (e) {
              // nothing else to do; row will reflect error
            } finally {
              if (segDir) await fs.rm(segDir, { recursive: true, force: true }).catch(() => {});
            }
          }
        }
      }
    } catch (e: any) {
      if (!status) status = "error";
      if (!errorStage) errorStage = "unexpected";
      if (!errorMessage) errorMessage = String(e?.message || e);
      console.error(`[${idx}/${detections.length}] error:`, errorMessage);
    }

    // Write one CSV row for this detection (ALWAYS)
    const flat = flattened[zeroIdx] ?? {};
    const row: Record<string, any> = {
      ...flat,
      windowStart,
      windowEnd,
      playlistTimestamps: JSON.stringify(playlistTimestamps),
      chunkFiles: JSON.stringify(chunkFiles),
      flacPath,
      status,
      errorStage,
      errorMessage,
    };
    await writeCsvLine(columns.map((k) => csvEscape(row[k])).join(",") + "\n");

    console.log(`[${idx}/${detections.length}] total ${took(Date.now() - t0)} | status=${status || "ok"}`);
  }

  // Run the pool
  await asyncPool(CONCURRENCY, detections, processDetection);

  csvStream.end();
  console.log(`\nCSV streaming complete → ${CSV_OUT}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
