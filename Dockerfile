FROM alpine:3.5

RUN apk add --no-cache ca-certificates

COPY ./dist/linux-amd64/extractor /

VOLUME ["/queries"]

CMD ["/extractor", "/conf/config.yml"]
