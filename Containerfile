FROM oven/bun:alpine AS builder
WORKDIR /app
COPY registry/package.json registry/bun.lock* ./
COPY common/common-spec /common-spec
RUN bun install
COPY registry/src/ src/
COPY registry/tsconfig.json ./
RUN bun build --compile src/main.ts --outfile registry

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/registry /app/registry
RUN chmod +x /app/registry
EXPOSE 25000
ENTRYPOINT ["/app/registry"]
