FROM alpine:3.4
MAINTAINER Matthew Endsley <mendsley@gmail.com>

COPY parchment.config.default /etc/parchment.config
CMD ["/usr/bin/parchment", "/etc/parchment.config"]

COPY parchmentcat /usr/bin/parchmentcat
COPY parchment /usr/bin/parchment
