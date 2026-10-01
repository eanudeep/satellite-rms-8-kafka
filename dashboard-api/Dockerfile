# Stage 1 — build: compile proto stubs + Go binary
FROM golang:1.22 AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
 && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

COPY go.mod satellite.proto main.go ./
COPY static/ ./static/
RUN protoc --go_out=. --go-grpc_out=. satellite.proto \
 && go mod tidy \
 && CGO_ENABLED=0 GOOS=linux go build -o app .

# Stage 2 — runtime: static binary only
FROM scratch
COPY --from=builder /app/app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
