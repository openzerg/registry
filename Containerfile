FROM oven/bun:alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY registry/package.json registry/bun.lock* ./
RUN bun install
COPY registry/src/ src/
COPY registry/tsconfig.json ./
RUN bun build --compile src/main.ts --outfile registry
FROM alpine:latest
RUN apk add --no-cache ca-certificates libstdc++
WORKDIR /app
COPY --from=builder /app/registry /app/registry
RUN chmod +x /app/registry
EXPOSE 25000
ENTRYPOINT ["/app/registry"]
