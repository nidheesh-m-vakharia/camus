FROM rust:1-bookworm AS build
WORKDIR /src

# Manifests only. This layer is reused until a dependency changes, so a
# source-only commit does not rebuild the third-party crates.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/protocol/Cargo.toml crates/protocol/Cargo.toml
COPY crates/raft/Cargo.toml crates/raft/Cargo.toml
COPY crates/metadata/Cargo.toml crates/metadata/Cargo.toml
COPY crates/storage/Cargo.toml crates/storage/Cargo.toml
COPY crates/broker/Cargo.toml crates/broker/Cargo.toml
COPY crates/wal/Cargo.toml crates/wal/Cargo.toml
COPY crates/uploader/Cargo.toml crates/uploader/Cargo.toml
COPY crates/coordinator/Cargo.toml crates/coordinator/Cargo.toml
COPY crates/compactor/Cargo.toml crates/compactor/Cargo.toml
COPY crates/cli/Cargo.toml crates/cli/Cargo.toml
COPY crates/engine-bench/Cargo.toml crates/engine-bench/Cargo.toml

RUN mkdir -p \
      crates/protocol/src \
      crates/raft/src \
      crates/metadata/src \
      crates/storage/src \
      crates/broker/src \
      crates/wal/src \
      crates/uploader/src \
      crates/coordinator/src \
      crates/compactor/src \
      crates/cli/src \
      crates/engine-bench/src \
    && printf '' > crates/protocol/src/lib.rs \
    && printf '' > crates/raft/src/lib.rs \
    && printf '' > crates/metadata/src/lib.rs \
    && printf '' > crates/storage/src/lib.rs \
    && printf 'fn main() {}\n' > crates/broker/src/main.rs \
    && printf '' > crates/wal/src/lib.rs \
    && printf '' > crates/uploader/src/lib.rs \
    && printf '' > crates/coordinator/src/lib.rs \
    && printf '' > crates/compactor/src/lib.rs \
    && printf 'fn main() {}\n' > crates/cli/src/main.rs \
    && printf 'fn main() {}\n' > crates/engine-bench/src/main.rs \
    && cargo build --release --locked -p camus-broker -p camus-cli

COPY crates crates
RUN cargo build --release --locked -p camus-broker -p camus-cli

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data --shell /usr/sbin/nologin camus \
    && mkdir -p /data \
    && chown camus:camus /data
COPY --from=build /src/target/release/camus-broker /usr/local/bin/camus-broker
COPY --from=build /src/target/release/camus /usr/local/bin/camus
COPY deploy/broker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
USER camus
VOLUME /data
EXPOSE 18001 9092
ENTRYPOINT ["docker-entrypoint.sh"]
