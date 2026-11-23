# Build context is the repository root:
#   docker build -f services/application/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Build in module mode: go.work lists modules that are not copied into this
# image; the replace directives in go.mod resolve pkg/ and gen/go/.
ENV GOWORK=off

COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY services/application/ services/application/

WORKDIR /src/services/application
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /server
COPY --from=build /src/services/application/migrations /migrations
EXPOSE 9004 8004
USER nonroot:nonroot
ENTRYPOINT ["/server"]
