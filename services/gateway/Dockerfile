# Build context is the repository root:
#   docker build -f services/gateway/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0

COPY go.work go.work.sum ./
COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY services/gateway/ services/gateway/

# go.work lists every service; drop the ones not copied into this image.
RUN go work edit -dropuse ./services/auth
WORKDIR /src/services/gateway
RUN go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/gateway /gateway
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/gateway"]
