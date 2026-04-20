FROM oven/bun:alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install
COPY src/ src/
COPY tsconfig.json ./
RUN bun build --compile src/main.ts --outfile registry
FROM alpine:latest
RUN apk add --no-cache ca-certificates libstdc++
WORKDIR /app
COPY --from=builder /app/registry /app/registry
RUN chmod +x /app/registry
EXPOSE 25000
ENTRYPOINT ["/app/registry"]
