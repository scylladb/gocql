FROM golang:1.21

ARG version
ARG fork
ENV version=${version}
ENV fork=${fork:-scylladb/scylla-bench}

RUN apt-get update && apt-get -y install --no-install-recommends unzip curl && apt-get clean &&  rm -rf /var/lib/apt/lists/*

RUN curl -Lo sb.zip https://github.com/${fork}/archive/refs/${version}.zip && \
    unzip sb.zip && \
    cd ./scylla-bench-* && \
    GO111MODULE=on go install . && \
    cd .. && \
    rm -f sb.zip