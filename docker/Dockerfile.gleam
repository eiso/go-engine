FROM alpine:latest
MAINTAINER Chris Lu <chris.lu@gmail.com>

RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
RUN apk --update add git openssh

EXPOSE 45326

VOLUME /data

COPY gleam /usr/bin/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
