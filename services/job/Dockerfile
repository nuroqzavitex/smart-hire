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
COPY contracts/events/ contracts/events/
COPY services/job/ services/job/

WORKDIR /src/services/job
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server &&     go build -trimpath -ldflags="-s -w" -o /out/projector ./cmd/projector

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /server
# The read-model projector (ADR 0009); compose runs it as job-projector.
COPY --from=build /out/projector /projector
COPY --from=build /src/services/job/migrations /migrations
# The projector validates job.* events against these JSON Schemas at
# runtime; eventschema.Dir() looks above the working directory.
COPY --from=build /src/contracts/events /contracts/events
WORKDIR /
EXPOSE 9002 8002
USER nonroot:nonroot
ENTRYPOINT ["/server"]
