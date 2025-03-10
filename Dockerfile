FROM rust:latest AS builder
WORKDIR /app

COPY Cargo.toml .
COPY Cargo.lock .
COPY ./src src 
COPY ./tests tests 

RUN cargo build --release

FROM ubuntu:latest AS xterm

RUN apt-get update && apt-get install -y xterm

FROM rust:latest AS release
WORKDIR /app

COPY --from=builder /app/target/release/node .
COPY --from=xterm /usr/bin/xterm /usr/bin/xterm
COPY users.json .
COPY schema.json .
COPY identity.pfx .
COPY key.pem .
COPY cert.pem .
COPY chain_certs.pem .
COPY node.sh .

ENV INTERNAL_PORT=8080
ENV EXTERNAL_PORT=8090

