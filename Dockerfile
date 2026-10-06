# One image per binary: docker build --build-arg CMD=orders .
# Cross-compiles on the build platform, so multi-arch images do not need emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
ARG CMD
LABEL org.opencontainers.image.source="https://github.com/sderosiaux/http-over-kafka" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.description="http-over-kafka ${CMD}"
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
