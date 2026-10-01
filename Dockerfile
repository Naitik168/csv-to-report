# syntax=docker/dockerfile:1.7
#
# One image, three binaries: `api`, `worker` and the `storage-admin` CLI. Compose picks one via `command`.
# Build toolchain and runtime share the same Debian release so glibc versions match.

# ---------- build ----------
FROM rust:1.95-slim-trixie AS builder
WORKDIR /app

# Cache the dependency build: compile deps against stub sources first.
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir -p src/bin \
 && echo "" > src/lib.rs \
 && echo "fn main() {}" > src/bin/api.rs \
 && echo "fn main() {}" > src/bin/worker.rs \
 && echo "fn main() {}" > src/bin/storage_admin.rs \
 && cargo build --release --locked \
 && rm -rf src

COPY migrations ./migrations
COPY src ./src
# `touch` so cargo notices the real sources are newer than the cached stubs.
RUN touch src/lib.rs src/bin/*.rs && cargo build --release --locked

# ---------- tests (used by `docker compose --profile test run --rm tests`) ----------
FROM builder AS tests
COPY tests ./tests
COPY samples ./samples
RUN cargo test --no-run --locked
CMD ["cargo", "test", "--locked", "--", "--test-threads=4"]

# ---------- runtime ----------
FROM debian:trixie-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --no-create-home app
COPY --from=builder /app/target/release/api /app/target/release/worker /app/target/release/storage-admin /usr/local/bin/
USER app
ENV LOG_FORMAT=json RUST_LOG=info
EXPOSE 8080
CMD ["api"]
