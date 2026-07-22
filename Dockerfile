FROM golang:1.26.5-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/greenthreads-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/greenthreads-server /greenthreads-server
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/greenthreads-server"]
CMD ["-listen", "0.0.0.0:8080"]
