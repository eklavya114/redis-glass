FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o redis-glass .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/redis-glass .
EXPOSE 6379 8080
CMD ["./redis-glass"]
