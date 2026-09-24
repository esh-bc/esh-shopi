FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY checkout.go ./
RUN go build -o checkout checkout.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/checkout .
ENV GOGC=50
ENV GOMEMLIMIT=800MiB
EXPOSE 8080
CMD ["./checkout"]
