# Build context is the repository root:
#   docker build -f services/auth/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Build in module mode: go.work lists modules that are not copied into this
# image; the replace directives in go.mod resolve pkg/ and gen/go/.
ENV GOWORK=off

COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY services/auth/ services/auth/

WORKDIR /src/services/auth
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && go build -trimpath -ldflags="-s -w" -o /out/seed ./cmd/seed

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /server
COPY --from=build /out/seed /seed
COPY --from=build /src/services/auth/migrations /migrations
EXPOSE 9001 8001
USER nonroot:nonroot
ENTRYPOINT ["/server"]
