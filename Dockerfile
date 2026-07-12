FROM cgr.dev/chainguard/go AS builder
COPY . /app
RUN cd /app && CGO_ENABLED=0 go build -o xtralink -ldflags="-s -w -X 'main.versionSuffix=-beta.$(date +%Y%m%d%H%M%S)'" .

FROM cgr.dev/chainguard/static
COPY --from=builder /app/xtralink /usr/bin/
ENTRYPOINT ["/usr/bin/xtralink"]
