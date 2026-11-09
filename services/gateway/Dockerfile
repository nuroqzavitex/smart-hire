# Build context is the repository root:
#   docker build -f services/gateway/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Build in module mode: go.work lists modules that are not copied into this
# image; the replace directives in go.mod resolve pkg/ and gen/go/.
ENV GOWORK=off

COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY services/gateway/ services/gateway/

WORKDIR /src/services/gateway
RUN go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/gateway"]
