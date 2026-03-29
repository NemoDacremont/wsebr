# 1.20
FROM rust:1-alpine3.23 AS builder

WORKDIR /build

# Required for build
# RUN apk add openssl-dev openssl-libs-static musl-dev

COPY Cargo.* .

RUN mkdir -p src/wsebrd && \
    echo "fn main() {}" > src/wsebrd/main.rs && \
    cargo build --target x86_64-unknown-linux-musl --release --bin wsebrd && \
    rm -rf ./src/ target/release/deps/wsebrd* target/release/wsebrd*

RUN apk add --no-cache sqlite-dev

COPY --parents ./src/ ./assets/ ./templates/ ./

RUN cargo build --release --bin wsebrd --target x86_64-unknown-linux-musl

FROM alpine:3.23

WORKDIR /app

RUN apk add --no-cache sqlite-libs

COPY ./assets/ ./templates/ ./

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/wsebrd .

CMD ["/app/wsebrd"]
