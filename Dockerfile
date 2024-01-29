FROM rustlang/rust:nightly-alpine3.17 AS builder

WORKDIR /app

RUN apk add --no-cache musl-dev
RUN cargo install cargo-watch

COPY . .

RUN cargo install --path .


FROM debian:buster-slim AS release

COPY --from=builder /usr/local/cargo/bin/yet-another-rust-database .
USER 1000

CMD ["/yet-another-rust-database"]