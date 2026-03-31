# 1.20
FROM rust:1-alpine3.23 AS builder

WORKDIR /build

# Required for build
# RUN apk add openssl-dev openssl-libs-static musl-dev

RUN apk add --no-cache sqlite-static sqlite-dev musl-dev build-base

COPY Cargo.* .

RUN mkdir -p src/wsebrd && \
    echo "fn main() {}" > src/wsebrd/main.rs && \
    cargo build --release --bin wsebrd && \
    rm -rf ./src/ target/release/deps/wsebrd* target/release/wsebrd*

COPY --parents ./src/ ./assets/ ./templates/ ./

RUN cargo build --release --bin wsebrd

FROM alpine:3.23

WORKDIR /app

COPY --parents ./assets/ ./templates/ ./

COPY --from=builder /build/target/release/wsebrd .

CMD ["/app/wsebrd"]
