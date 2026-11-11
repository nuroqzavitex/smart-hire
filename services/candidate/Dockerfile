# Build context is the repository root:
#   docker build -f services/candidate/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
ENV CGO_ENABLED=0
# Build in module mode: go.work lists modules that are not copied into this
# image; the replace directives in go.mod resolve pkg/ and gen/go/.
ENV GOWORK=off

COPY pkg/ pkg/
COPY gen/go/ gen/go/
COPY contracts/events/ contracts/events/
COPY services/candidate/ services/candidate/

WORKDIR /src/services/candidate
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/server /server
COPY --from=build /src/services/candidate/migrations /migrations
# The result consumer validates cv.* events against these JSON Schemas at
# runtime; eventschema.Dir() looks for contracts/events above the working
# directory, so the image runs from /.
COPY --from=build /src/contracts/events /contracts/events
WORKDIR /
EXPOSE 9003 8003
USER nonroot:nonroot
ENTRYPOINT ["/server"]
