FROM alpine:3.5

RUN apk add --no-cache ca-certificates

COPY ./dist/linux-amd64/sql-extractor /

VOLUME ["/queries"]

CMD ["/sql-extractor", "/conf/config.yml"]
