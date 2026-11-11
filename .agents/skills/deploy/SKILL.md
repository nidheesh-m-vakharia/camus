---
name: deploy
description: Build and run the Camus Docker image, docker compose, the Railway deployment, or the GHCR image published by CI.
---

# Deploy

- **Image:** `docker build -t camus .`, then
  `docker run --rm -p 18001:18001 -p 9092:9092 camus`. The entrypoint is
  `deploy/broker/docker-entrypoint.sh`. It initializes a one-node group.
- **Compose:** `docker compose up -d minio create-bucket` starts the bucket
  only. Add `--profile brokers` for three brokers. MinIO builds from
  `deploy/minio`.
- **GHCR:** after the test job passes on `main`, `.github/workflows/ci.yml`
  pushes `ghcr.io/<owner>/camus:latest` and `:<sha>`. PRs build without
  pushing.
- **Railway:** `.railway/railway.ts` defines one broker, a `/data` volume,
  bucket `logs`, and a TCP proxy on 9092. Run
  `npm install --prefix .railway`, `railway link`, `railway config plan`,
  then `railway config apply`. Admin HTTP stays private on `PORT`.
- Deploys and pushes reach shared infrastructure. Run `plan`, show the
  output, and get a yes before you `apply`.
