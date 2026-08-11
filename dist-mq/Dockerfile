# Stage 1: build
FROM golang:1.25.4 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o dist-mq .

# Stage 2: run
FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/dist-mq .
ENTRYPOINT ["./dist-mq"]