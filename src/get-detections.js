// get-detections.js
// Node 18+ required (global fetch). Run with:
//   node get-detections.js --since 2024-11-13T00:00:00Z
//   node get-detections.js --all
// Optional:
//   --endpoint https://live.orcasound.net/graphql
//   --limit 250
//   --out json/detections.json
//   --nodeName rpi_sunset_bay     <-- filter by feed.nodeName (case-insensitive)

const fs = require("node:fs/promises");
const path = require("node:path");

// -------- CLI flags --------
const argv = process.argv.slice(2);
function flag(name, def = undefined) {
  const i = argv.indexOf(`--${name}`);
  if (i === -1) return def;
  const v = argv[i + 1];
  if (!v || v.startsWith("--")) return true; // boolean flag
  return v;
}

const ENDPOINT = flag("endpoint", process.env.ORCASOUND_GQL || "https://live.orcasound.net/graphql");
const LIMIT = Number(flag("limit", 250));
const SINCE_ISO = flag("since", null);
const ALL = !!flag("all", false);
const OUT_PATH = flag("out", path.join(process.cwd(), "json", "detections.json"));
const NODE_NAME = flag("nodeName", null); // optional filter

// -------- GraphQL --------
const QUERY = `
  query DetectionsPaged($limit: Int!, $offset: Int) {
    detections(limit: $limit, offset: $offset) {
      results {
        id
        feedId
        listenerCount
        category
        description
        playerOffset
        playlistTimestamp
        timestamp
        candidate { id feedId }
        feed { id name nodeName slug }
      }
    }
  }
`;

// Minimal retry helper (handles transient 5xx/429)
async function fetchJSON(url, init, retries = 3) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, init);
      const ct = res.headers.get("content-type") || "";
      const text = await res.text();
      if (!res.ok) throw new Error(`HTTP ${res.status} – ${text.slice(0, 300)}`);
      if (!ct.includes("application/json")) {
        throw new Error(
          `Non-JSON response (are you pointing at /graphql?):\n${text.slice(0, 200)}`
        );
      }
      return JSON.parse(text);
    } catch (e) {
      lastErr = e;
      // small backoff
      await new Promise((r) => setTimeout(r, 250 * (i + 1)));
    }
  }
  throw lastErr;
}

async function gql(query, variables = {}) {
  const json = await fetchJSON(ENDPOINT, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });

  if (json.errors?.length) {
    throw new Error(`GraphQL errors:\n${JSON.stringify(json.errors, null, 2)}`);
  }
  return json.data;
}

// -------- time helpers --------
function parseSinceMillis() {
  if (ALL) return null; // no cutoff
  if (!SINCE_ISO) return null;
  const d = new Date(SINCE_ISO);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`Bad ISO date for --since: ${SINCE_ISO}`);
  }
  return d.getTime();
}

function detectionMillis(det) {
  // Prefer ISO timestamp if present, else fall back to playlistTimestamp (seconds)
  if (det.timestamp) {
    const t = new Date(det.timestamp).getTime();
    if (!Number.isNaN(t)) return t;
  }
  if (Number.isFinite(det.playlistTimestamp)) {
    return Number(det.playlistTimestamp) * 1000; // IMPORTANT: seconds → ms
  }
  return NaN;
}

function matchesNodeName(det, want) {
  if (!want) return true; // no filter
  const have = det?.feed?.nodeName;
  return typeof have === "string" && have.toLowerCase() === String(want).toLowerCase();
}

async function fetchAllDetections() {
  const cutoff = parseSinceMillis();

  const all = [];
  const seen = new Set(); // de-dupe by id if pages overlap
  let offset = 0;
  let pageNo = 0;

  for (;;) {
    pageNo++;
    const data = await gql(QUERY, { limit: LIMIT, offset });
    const page = data?.detections?.results ?? [];

    if (page.length === 0) break;

    // Append filtered items
    for (const det of page) {
      if (!det?.id || seen.has(det.id)) continue;

      // nodeName filter (client-side)
      if (!matchesNodeName(det, NODE_NAME)) continue;

      if (cutoff == null) {
        all.push(det);
        seen.add(det.id);
      } else {
        const ms = detectionMillis(det);
        if (!Number.isNaN(ms) && ms >= cutoff) {
          all.push(det);
          seen.add(det.id);
        }
      }
    }

    // Heuristics to end the loop
    if (page.length < LIMIT) break; // last page
    offset += LIMIT;

    // Politeness
    await new Promise((r) => setTimeout(r, 120));
  }

  return all;
}

async function main() {
  try {
    const outDir = path.dirname(OUT_PATH);
    await fs.mkdir(outDir, { recursive: true });

    console.log(
      `Querying detections @ ${ENDPOINT}\n` +
        (ALL ? "  (all time)\n" : `  since: ${SINCE_ISO}\n`) +
        (NODE_NAME ? `  nodeName filter: ${NODE_NAME}\n` : "  nodeName filter: (none)\n") +
        `  limit/page: ${LIMIT}\n  output: ${OUT_PATH}`
    );

    const detections = await fetchAllDetections();

    // Sort ascending by primary ISO timestamp (fallback to playlistTimestamp)
    detections.sort((a, b) => {
      const ams = detectionMillis(a);
      const bms = detectionMillis(b);
      return (ams || 0) - (bms || 0);
    });

    const payload = {
      since: ALL ? null : SINCE_ISO,
      nodeName: NODE_NAME || null,
      count: detections.length,
      generatedAt: new Date().toISOString(),
      detections,
    };

    await fs.writeFile(OUT_PATH, JSON.stringify(payload, null, 2));
    console.log(`Saved ${detections.length} detections → ${OUT_PATH}`);
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes("Non-JSON response")) {
      console.error(msg);
      console.error("Hint: switch to the real endpoint: --endpoint https://live.orcasound.net/graphql");
    } else if (msg.includes("Unknown argument") && msg.includes("offset")) {
      console.error("This API may not support `offset:`. Change the query to use `skip:` or cursor pagination.");
      console.error("Example:\n  detections(limit: $limit, skip: $offset) { ... }");
    } else {
      console.error(err);
    }
    process.exit(1);
  }
}

main();
