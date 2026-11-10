# Build context is the repository root:
#   docker build -f services/job/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Build in module mode: go.work lists modules that are not copied into this
# image; the replace directives in go.mod resolve pkg/ and gen/go/.
ENV GOWORK=off

COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY services/job/ services/job/

WORKDIR /src/services/job
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /server
COPY --from=build /src/services/job/migrations /migrations
EXPOSE 9002 8002
USER nonroot:nonroot
ENTRYPOINT ["/server"]
