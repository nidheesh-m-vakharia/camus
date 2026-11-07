FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
RUN cargo build --release --locked -p camus-broker

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data --shell /usr/sbin/nologin camus \
    && mkdir -p /data \
    && chown camus:camus /data
COPY --from=build /src/target/release/camus-broker /usr/local/bin/camus-broker
COPY deploy/broker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
USER camus
VOLUME /data
EXPOSE 18001 9092
ENTRYPOINT ["docker-entrypoint.sh"]
