# Base images are pinned to immutable digests for reproducible, tamper-evident
# builds (OpenSSF Scorecard "Pinned-Dependencies"). The human-readable tag is
# kept in the comment. To update: docker pull <image>:<tag> &&
#   docker inspect --format='{{index .RepoDigests 0}}' <image>:<tag>
# then replace the digest below. Dependabot's Docker ecosystem also bumps these.

# golang:1.26-alpine
FROM golang@sha256:3aff6657219a4d9c14e27fb1d8976c49c29fddb70ba835014f477e1c70636647 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# VERSION is injected by the release workflow so the binary self-reports its
# release tag (greenthreads-server -version). Defaults to "docker" for local
# builds that do not pass the arg.
ARG VERSION=docker
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/greenthreads-server ./cmd/server

# gcr.io/distroless/static-debian12:nonroot
FROM gcr.io/distroless/static-debian12@sha256:a9fcaedd4c9b59e12dd65d954f0b5044f19b0647a8a3712e77205df9e7b102cd

COPY --from=build /out/greenthreads-server /greenthreads-server
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/greenthreads-server"]
CMD ["-listen", "0.0.0.0:8080"]
