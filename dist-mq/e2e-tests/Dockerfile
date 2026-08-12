# Both e2e workloads in one image. The durability suite is a compiled test
# binary rather than a `go test` invocation so the runtime image needs no
# toolchain and no source.
#
# Build from the module root: docker build -f e2e-tests/Dockerfile .
FROM golang:1.25.4 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/sink ./e2e-tests/cmd/sink
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/bench ./e2e-tests/cmd/bench
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/aggregate ./e2e-tests/cmd/aggregate
RUN CGO_ENABLED=0 GOOS=linux go test -c -o /out/durability.test ./e2e-tests/durability

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/sink /out/bench /out/aggregate /out/durability.test ./
