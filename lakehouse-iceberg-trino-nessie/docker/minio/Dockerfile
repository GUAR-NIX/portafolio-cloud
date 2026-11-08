# MinIO compilado desde el código fuente oficial (AGPL-3.0). Desde fines de 2025 MinIO ya no publica imágenes en
# Docker Hub ni en quay.io, así que se compila la última versión etiquetada del servidor y del cliente `mc`.
FROM golang:1.24-alpine AS compilacion
RUN apk add --no-cache git
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
RUN git clone --depth 1 --branch ${MINIO_VERSION} https://github.com/minio/minio /src/minio \
 && cd /src/minio && go build -ldflags "-s -w" -o /salida/minio .
RUN git clone --depth 1 --branch ${MC_VERSION} https://github.com/minio/mc /src/mc \
 && cd /src/mc && go build -ldflags "-s -w" -o /salida/mc .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=compilacion /salida/minio /salida/mc /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
