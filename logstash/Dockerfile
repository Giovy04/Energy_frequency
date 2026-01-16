FROM docker.elastic.co/logstash/logstash:8.12.2

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
USER logstash
