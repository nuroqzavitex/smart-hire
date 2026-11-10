# MinIO server + mc built from the official GitHub source at pinned release
# tags (ruling E-R2, ADR 0008): the upstream Docker Hub images no longer exist
# and no third-party/community images are used. One image serves both the
# minio service and the one-shot minio-init (mc) container.
ARG GO_IMAGE=golang:1.27-bookworm

FROM ${GO_IMAGE} AS build
ARG MINIO_TAG=RELEASE.2025-04-22T22-12-26Z
ARG MC_TAG=RELEASE.2025-04-16T18-13-26Z
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
WORKDIR /src
RUN git clone --depth 1 --branch "${MINIO_TAG}" https://github.com/minio/minio.git minio \
 && git clone --depth 1 --branch "${MC_TAG}" https://github.com/minio/mc.git mc
# Stamp version/release tag like the upstream release builds do
# (buildscripts/gen-ldflags.go takes the RFC3339 release time, e.g.
# RELEASE.2025-04-22T22-12-26Z -> 2025-04-22T22:12:26Z). The assignments fail
# the step if ldflag generation fails, instead of silently building unstamped.
RUN cd minio \
 && ver=$(echo "${MINIO_TAG#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/') \
 && ldf=$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "$ver") \
 && go build -tags kqueue -ldflags "$ldf" -o /out/minio .
RUN cd mc \
 && ver=$(echo "${MC_TAG#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/') \
 && ldf=$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "$ver") \
 && go build -tags kqueue -ldflags "$ldf" -o /out/mc .

FROM debian:bookworm-slim
COPY --from=build /out/minio /out/mc /usr/bin/
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
