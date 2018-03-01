FROM alpine:latest
MAINTAINER Chris Lu <chris.lu@gmail.com>

RUN mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2
RUN apk --update add git openssh

EXPOSE 45326

VOLUME /tmp

ENV PATH="${PATH}:."

COPY gleam /usr/bin/
COPY docker/entrypoint_gleam.sh /entrypoint_gleam.sh

RUN chmod +x /entrypoint_gleam.sh

ENTRYPOINT ["/entrypoint_gleam.sh"]
