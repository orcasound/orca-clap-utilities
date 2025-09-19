```
node get-detections.js --since 2024-11-13T00:00:00Z
node get-detections.js --all
// Optional:
//   --endpoint https://live.orcasound.net/graphql
//   --limit 250
//   --out json/detections.json
//   --nodeName rpi_sunset_bay  

```

```

npx ts-node src/pairs-from-hls.ts json/example-detections.json \
  --via ffmpeg \
  --toS3 \
  --bucket acoustic-sandbox \
  --prefix clap-model/ \
  --region us-east-2 \
  --skipIfExists \
  --appendCsv true \
  --resumeCsv \
  --concurrency 10 \
  --encodeConcurrency 4 \
  --flacLevel 3

```

```
npx ts-node pairs-from-flac.ts <detections.json> [--fmt wav|flac] [--csvOnly]

```