FROM rust:1.97-bookworm AS builder

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs && cargo build --locked --release

COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home kepos-codex-bridge

COPY --from=builder --chown=10001:10001 /src/target/release/kepos-codex-bridge /usr/local/bin/kepos-codex-bridge

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/kepos-codex-bridge"]
