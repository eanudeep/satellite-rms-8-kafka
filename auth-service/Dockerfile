# Stage 1 — build: pure HTTP service, no proto compilation needed
FROM golang:1.22 AS builder
WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o app .

# Stage 2 — runtime: static binary only
FROM scratch
COPY --from=builder /app/app /app
EXPOSE 8081
ENTRYPOINT ["/app"]
