# Base image tags intentionally specify minor versions (not patch) to allow
# automatic security patch updates in the runner. Pin to full digests in
# production by running:
#   docker pull <image>:<tag> && docker inspect --format='{{index .RepoDigests 0}}' <image>:<tag>

# Pin to golang:1.26-alpine. To update: docker pull golang:1.26-alpine && docker inspect --format='{{index .RepoDigests 0}}' golang:1.26-alpine
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/greenthreads-server ./cmd/server

# Pin to gcr.io/distroless/static-debian12:nonroot. To update: docker pull gcr.io/distroless/static-debian12:nonroot && docker inspect --format='{{index .RepoDigests 0}}' gcr.io/distroless/static-debian12:nonroot
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/greenthreads-server /greenthreads-server
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/greenthreads-server"]
CMD ["-listen", "0.0.0.0:8080"]
