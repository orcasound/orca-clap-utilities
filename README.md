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