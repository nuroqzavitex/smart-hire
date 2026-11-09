# Build context is the repository root:
#   docker build -f services/auth/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0

COPY go.work go.work.sum ./
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
