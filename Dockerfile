FROM ubuntu:16.04
MAINTAINER Ravi Teja <k.teza1@gmail.com>
ADD target/debug/rumqttd /usr/bin/rumqttd
ADD conf/rumqttd.conf /etc/rumqttd.conf
ENV RUST_BACKTRACE=1
ENTRYPOINT ["rumqttd"]
