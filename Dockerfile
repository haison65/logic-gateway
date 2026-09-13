# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/http2gw ./cmd/http2gw \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/logic ./cmd/logic \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/master ./cmd/master \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

FROM gcr.io/distroless/static-debian12:nonroot AS http2gw
WORKDIR /
COPY --from=build --chown=nonroot:nonroot /out/http2gw /http2gw
COPY --chown=nonroot:nonroot configs/http2gw.docker.yaml /configs/http2gw.yaml
USER nonroot:nonroot
EXPOSE 8080/tcp
EXPOSE 9000/udp
ENTRYPOINT ["/http2gw"]
CMD ["-config", "/configs/http2gw.yaml"]

FROM gcr.io/distroless/static-debian12:nonroot AS logic
WORKDIR /
COPY --from=build --chown=nonroot:nonroot /out/logic /logic
COPY --chown=nonroot:nonroot configs/logic.docker.yaml /configs/logic.yaml
USER nonroot:nonroot
EXPOSE 9100/udp
EXPOSE 9101/udp
ENTRYPOINT ["/logic"]
CMD ["-config", "/configs/logic.yaml"]

FROM gcr.io/distroless/static-debian12:nonroot AS master
WORKDIR /
COPY --from=build --chown=nonroot:nonroot /out/master /master
COPY --chown=nonroot:nonroot configs/local/master.docker.yaml /configs/master.yaml
USER nonroot:nonroot
EXPOSE 9200/tcp
ENTRYPOINT ["/master"]
CMD ["-config", "/configs/master.yaml"]

FROM gcr.io/distroless/static-debian12:nonroot AS client
WORKDIR /
COPY --from=build --chown=nonroot:nonroot /out/client /client
USER nonroot:nonroot
ENTRYPOINT ["/client"]
CMD ["-config", "/configs/client.yaml"]
