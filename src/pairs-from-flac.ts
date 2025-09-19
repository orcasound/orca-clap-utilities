/**
 * pairs-from-flac.ts
 *
 * Concatenate Orcasound 30s FLAC chunks for timestamps ± buffer,
 * detecting per-UTC-day phase (chunk second offsets) automatically.
 * Emits a CSV with detection fields + outPath (chosen via --fmt) + chunkFiles[].
 *
 * New: --csvOnly  -> only write CSV (no downloads, no ffmpeg). chunkFiles still listed.
 *
 * Usage:
 *   npx ts-node pairs-from-flac.ts <detections.json> [--fmt wav|flac] [--csvOnly]
 *
 * Requires: Node 18+ (global fetch), ffmpeg on PATH (unless --csvOnly)
 */

import { execFile } from "node:child_process";
import { createWriteStream, promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join, basename, dirname as pathDirname } from "node:path";
import { pipeline } from "node:stream/promises";

// -------------------- USER CONFIG --------------------
const FEED_NAME = process.argv[3];
const BUFFER_SEC = 30;
// Static per-site hint used ONLY as fallback if a day's phase can't be auto-detected:
const SITE_PHASE_HINT: Record<string, number> = {
  rpi_sunset_bay: 2,
  rpi_port_townsend: 8,
};
// ----------------------------------------------------

// Archive details
const SUFFIX = "-48000-1.flac";
const SEG_SECONDS = 30;
const BASE_PREFIX = `https://audio-orcasound-net.s3.amazonaws.com/${FEED_NAME}/flac`;

// ---------- CLI ----------
const jsonPath = process.argv[2];
if (!jsonPath || jsonPath.startsWith("--")) {
  console.error("Usage: npx ts-node flac-concat-v2.ts <detections.json> [--fmt wav|flac] [--csvOnly]");
  process.exit(1);
}

// parse flags
let OUTPUT_FMT: "wav" | "flac" = "wav";
let CSV_ONLY = false;
for (let i = 3; i < process.argv.length; i++) {
  const arg = process.argv[i];
  if (arg === "--fmt") {
    const val = String(process.argv[i + 1] || "").toLowerCase();
    if (val === "wav" || val === "flac") OUTPUT_FMT = val;
    else {
      console.error(`Invalid --fmt value "${val}". Use "wav" or "flac".`);
      process.exit(1);
    }
    i++;
  } else if (arg === "--csvOnly") {
    CSV_ONLY = true;
  }
}

// ---------- small utils ----------
const exists = async (p: string) => fs.access(p).then(() => true).catch(() => false);

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
    delete flat.feed;
  }
  if (det.candidate && typeof det.candidate === "object") {
    flat["candidate.id"] = det.candidate.id ?? "";
    flat["candidate.feedId"] = det.candidate.feedId ?? "";
    delete flat.candidate;
  }
  return flat;
}

// ---------- Time helpers ----------
function toUtcDate(x: string | Date): Date {
  let s = typeof x === "string" ? x : x.toISOString();

  // Normalize dashy time (e.g., 'T22-32-30' → 'T22:32:30')
  if (/T\d{2}-\d{2}-\d{2}/.test(s)) {
    s = s.replace(/T(\d{2})-(\d{2})-(\d{2})/, "T$1:$2:$3");
  }
  // Allow ' ' between date/time just in case → ISO 'T'
  s = s.replace(" ", "T");
  // Trim microseconds beyond ms
  const fixed = s.replace(/(\.\d{3})\d+(Z|[+\-]\d\d:\d\d)$/, "$1$2");
  const d = new Date(fixed);

  if (Number.isNaN(d.getTime())) throw new Error(`Invalid date: ${s}`);
  return d;
}

function fmtNameStamp(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  const Y = d.getUTCFullYear();
  const M = pad(d.getUTCMonth() + 1);
  const D = pad(d.getUTCDate());
  const h = pad(d.getUTCHours());
  const m = pad(d.getUTCMinutes());
  const s = pad(d.getUTCSeconds());
  return `${Y}-${M}-${D}_${h}-${m}-${s}`;
}

function safeIsoForFile(d: Date): string {
  return d.toISOString().replace(/:/g, "-").replace(/\//g, "-");
}

function floorToSegPhase(d: Date, segSec: number, phaseSec: number): Date {
  const t = d.getTime(), step = segSec * 1000, phaseMs = phaseSec * 1000;
  return new Date(Math.floor((t - phaseMs) / step) * step + phaseMs);
}
function ceilToSegPhase(d: Date, segSec: number, phaseSec: number): Date {
  const t = d.getTime(), step = segSec * 1000, phaseMs = phaseSec * 1000;
  return new Date(Math.ceil((t - phaseMs) / step) * step + phaseMs);
}

function minuteStartUTC(d: Date): Date {
  return new Date(Date.UTC(
    d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(),
    d.getUTCHours(), d.getUTCMinutes(), 0
  ));
}

// ---------- URL/name helpers ----------
function buildNameFor(site: string, when: Date): string {
  return `${fmtNameStamp(when)}_${site}${SUFFIX}`;
}
function fileUrl(fullName: string): string {
  return `${BASE_PREFIX}/${encodeURIComponent(fullName)}`;
}

// ---------- Fast existence probe ----------
async function objectExistsFast(url: string, timeoutMs = 8000): Promise<boolean> {
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, { method: "GET", headers: { Range: "bytes=0-0" }, signal: ac.signal });
    if (!res.ok || !res.body) return false;
    const ct = res.headers.get("content-type") || "";
    if (ct.includes("text/html")) return false;
    return true;
  } catch {
    return false;
  } finally {
    clearTimeout(to);
  }
}

// ---------- Per-day phase detection (cached) ----------
const DAY_PHASE_CACHE = new Map<string, number>();
function dayKeyUTC(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

/**
 * Detect phase for the UTC day of `ref`, by probing the actual archive
 * at the minute of `ref`. Returns s%30 (e.g., 8, 21, 22) or null.
 */
async function detectPhaseForDay(site: string, ref: Date): Promise<number | null> {
  const key = dayKeyUTC(ref);
  if (DAY_PHASE_CACHE.has(key)) return DAY_PHASE_CACHE.get(key)!;

  const m0 = minuteStartUTC(ref);

  // Likely offsets first (based on observed patterns), then the rest
  const preferred = [2, 8, 21, 22, 32, 38, 51, 52];
  const rest = Array.from({ length: 60 }, (_, i) => i).filter(s => !preferred.includes(s));
  const order = [...preferred, ...rest];

  for (const s of order) {
    const ts = new Date(m0.getTime() + s * 1000);
    const name = buildNameFor(site, ts);
    const url = fileUrl(name);
    if (await objectExistsFast(url)) {
      const phase = s % 30;
      DAY_PHASE_CACHE.set(key, phase);
      if (process.env.DEBUG?.toLowerCase() === "1") {
        console.log(`[PHASE] ${site} ${key} → +${phase}s via ${name}`);
      }
      return phase;
    }
  }

  if (process.env.DEBUG?.toLowerCase() === "1") {
    console.warn(`[PHASE] ${site} ${key} → no audio found in minute ${fmtNameStamp(m0)}Z`);
  }
  return null;
}

// ---------- Fileset generation (using a phase) ----------
function enumerateChunkNames(startISO: string, endISO: string, site: string, phaseSec: number): string[] {
  const start = toUtcDate(startISO);
  const end = toUtcDate(endISO);
  if (end <= start) throw new Error("End must be after start");

  let t = floorToSegPhase(start, SEG_SECONDS, phaseSec);
  const endCeil = ceilToSegPhase(end, SEG_SECONDS, phaseSec);
  const names: string[] = [];

  while (t < endCeil) {
    const name = buildNameFor(site, t);
    names.push(name);
    t = new Date(t.getTime() + SEG_SECONDS * 1000);
  }
  return names;
}

// ---------- Download (used only when not --csvOnly) ----------
async function downloadIfExists(url: string, dest: string): Promise<boolean> {
  try {
    const res = await fetch(url, { method: "GET" }); // full file (no Range)
    if (!res.ok || !res.body) return false;

    // Guard against HTML index/error documents
    const ct = res.headers.get("content-type") || "";
    if (ct.includes("text/html")) return false;

    await fs.mkdir(pathDirname(dest), { recursive: true }).catch(() => {});
    const out = createWriteStream(dest);
    await pipeline(res.body as any, out);
    return true;
  } catch {
    return false;
  }
}

// ---------- ffmpeg concat (used only when not --csvOnly) ----------
async function runFfmpegConcat(listPath: string, outputPath: string, kind: "wav" | "flac"): Promise<void> {
  return new Promise((resolve, reject) => {
    const baseArgs = [
      "-y", // force overwrite so duplicates don't hang
      "-f", "concat", "-safe", "0",
      "-analyzeduration", "100M", "-probesize", "100M",
      "-i", listPath
    ];
    const args =
      kind === "wav"
        ? [...baseArgs, "-c:a", "pcm_s16le", outputPath]
        : [...baseArgs, "-c:a", "flac", "-compression_level", "5", outputPath];

    execFile("ffmpeg", args, (err, _stdout, stderr) => {
      if (err) reject(new Error(`ffmpeg failed (${err.message}). stderr:\n${stderr?.toString() ?? ""}`));
      else resolve();
    });
  });
}

// ---------- Core per-timestamp build ----------
type BuiltOutputs = {
  outPath: string;           // chosen format path (blank in csvOnly)
  windowStart: string;
  windowEnd: string;
  chunkFiles: string[];      // archive FLAC filenames covering the window
};

async function buildForTimestamp(
  feedName: string,
  tsIso: string | null,
  bufferSec: number,
  tmpRoot: string,
  outDir: string,
  csvOnly: boolean
): Promise<BuiltOutputs | null> {
  if (!tsIso) {
    return { outPath: "", windowStart: "", windowEnd: "", chunkFiles: [] };
  }

  const center = toUtcDate(tsIso);
  const start = new Date(center.getTime() - bufferSec * 1000);
  const end = new Date(center.getTime() + bufferSec * 1000);
  const windowStart = start.toISOString();
  const windowEnd = end.toISOString();

  // Detect this UTC day's phase once; fall back to hint if needed
  let phase = await detectPhaseForDay(feedName, center);
  if (phase == null) {
    phase = Number.isFinite(SITE_PHASE_HINT[feedName]) ? SITE_PHASE_HINT[feedName] : 2;
    if (process.env.DEBUG?.toLowerCase() === "1") {
      console.warn(`[PHASE] fallback for ${feedName} @ ${dayKeyUTC(center)} → +${phase}s`);
    }
  }

  // Enumerate chunk names FIRST (used in CSV in all modes)
  const names = enumerateChunkNames(windowStart, windowEnd, feedName, phase);
  if (!names.length) {
    console.warn("No chunks enumerated for", tsIso);
    return { outPath: "", windowStart, windowEnd, chunkFiles: [] };
  }

  // If CSV-only, we stop here—no downloads, no ffmpeg.
  if (csvOnly) {
    return { outPath: "", windowStart, windowEnd, chunkFiles: names };
  }

  // Compute output path for the chosen format and maybe skip if exists
  await fs.mkdir(outDir, { recursive: true });
  const startIsoSafe = safeIsoForFile(start);
  const endIsoSafe = safeIsoForFile(end);
  const outBase = `${feedName}_${startIsoSafe}_${endIsoSafe}`;
  const chosenExt = OUTPUT_FMT === "wav" ? ".wav" : ".flac";
  const outPath = join(outDir, `${outBase}${chosenExt}`);

  if (await exists(outPath)) {
    if (process.env.DEBUG?.toLowerCase() === "1") {
      console.log(`↷ skip (exists): ${outBase}${chosenExt}`);
    }
    // We still return chunk names that *should* cover the window
    return { outPath, windowStart, windowEnd, chunkFiles: names };
  }

  // Otherwise download and build
  const dlDir = join(tmpRoot, `dl_${feedName}_${fmtNameStamp(center)}`);
  await fs.mkdir(dlDir, { recursive: true });

  const presentPaths: string[] = [];
  const usedNames: string[] = [];
  for (const name of names) {
    const url = fileUrl(name);
    const dest = join(dlDir, basename(name));
    const ok = await downloadIfExists(url, dest);
    if (ok) {
      presentPaths.push(dest);
      usedNames.push(name);
    } else {
      console.warn("Missing chunk:", name);
    }
  }

  if (presentPaths.length === 0) {
    console.warn("No chunks found for timestamp:", tsIso);
    return { outPath: "", windowStart, windowEnd, chunkFiles: [] };
  }

  // Concat list and build chosen format
  const listTxt = presentPaths.map((p) => `file '${p.replace(/'/g, "'\\''")}'`).join("\n");
  const listPath = join(dlDir, "list.txt");
  await fs.writeFile(listPath, listTxt, "utf8");
  await runFfmpegConcat(listPath, outPath, OUTPUT_FMT);

  return { outPath, windowStart, windowEnd, chunkFiles: usedNames };
}

// ---------- Main ----------
async function main() {
  // Load detections JSON → timestamps array
  const raw = await fs.readFile(jsonPath, "utf8");
  const parsed = JSON.parse(raw);
  const detections: any[] = parsed.detections ?? [];
  const timestamps: (string | null)[] = detections.map((d) => d.timestamp ?? null);

  console.log(
    `Feed=${FEED_NAME} | Mode=${CSV_ONLY ? "CSV-only" : OUTPUT_FMT} | Loaded ${timestamps.length} detections from ${jsonPath}`
  );

  const outDir = join(process.cwd(), `${FEED_NAME}_outputs`);
  await fs.mkdir(outDir, { recursive: true });

  const CSV_OUT = join(outDir, `${FEED_NAME}_detections_with_paths_${CSV_ONLY ? "csvOnly" : OUTPUT_FMT}.csv`);
  const tmpRoot = await fs.mkdtemp(join(tmpdir(), "orcasound-flac-"));

  // Prepare CSV header: union of flattened detection keys (stable order)
  const flattened = detections.map(flattenDetection);
  const keySet = new Set<string>();
  for (const f of flattened) Object.keys(f).forEach((k) => keySet.add(k));
  const baseColumns = Array.from(keySet).sort();
  const columns = [...baseColumns, "windowStart", "windowEnd", "outPath", "chunkFiles"];

  const lines: string[] = [];
  lines.push(columns.map(csvEscape).join(",")); // header

  let idx = 0;
  for (const det of detections) {
    const ts: string | null = det.timestamp ?? null;
    if ((idx % 25) === 0) {
      console.log(`[${idx + 1}/${detections.length}] ${ts ?? "(no timestamp)"}${CSV_ONLY ? " (csvOnly)" : ""}`);
    }

    const flat = flattened[idx];
    let outPath = "";
    let windowStart = "";
    let windowEnd = "";
    let chunkFiles: string[] = [];

    try {
      const built = await buildForTimestamp(FEED_NAME, ts, BUFFER_SEC, tmpRoot, outDir, CSV_ONLY);
      if (built) {
        outPath = built.outPath;
        windowStart = built.windowStart;
        windowEnd = built.windowEnd;
        chunkFiles = built.chunkFiles;
        if (CSV_ONLY) {
          console.log("•", ts, "→", `${chunkFiles.length} segments`);
        } else if (built.outPath) {
          console.log("✓", ts, "→", built.outPath);
        } else {
          console.log("—", ts, "(no audio / skipped)");
        }
      }
    } catch (e: any) {
      console.error("✗", ts, e?.message || e);
    }

    const row: Record<string, any> = {
      ...flat,
      windowStart,
      windowEnd,
      outPath,
      chunkFiles: JSON.stringify(chunkFiles),
    };
    const line = columns.map((k) => csvEscape(row[k]));
    lines.push(line.join(","));

    idx++;
  }

  await fs.writeFile(CSV_OUT, lines.join("\n"), "utf8");
  console.log(`\nCSV written: ${CSV_OUT}`);
  console.log(`Outputs directory: ${outDir}${CSV_ONLY ? " (no audio files written in csvOnly mode)" : ""}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
