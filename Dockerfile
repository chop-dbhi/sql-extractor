FROM alpine:3.5

RUN apk add --no-cache ca-certificates

COPY ./dist/linux-amd64/sql-extractor /

VOLUME ["/queries"]
VOLUME ["/data"]

ENTRYPOINT ["/sql-extractor"]

CMD ["/config.yml"]
