# Community MinIO images are no longer published (dl.min.io returns 410,
# Docker Hub and Quay deny anonymous pulls). Build the archived release
# the project pins, then run it under OrbStack.
FROM golang:1.25-bookworm AS build
RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z \
 && go install github.com/minio/mc@RELEASE.2025-08-13T08-35-41Z

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /go/bin/minio /usr/local/bin/minio
COPY --from=build /go/bin/mc /usr/local/bin/mc
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
