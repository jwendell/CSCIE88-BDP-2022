FROM fedora:36

ENV FILEBEAT_VERSION=8.4.3

RUN curl -sfL https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-${FILEBEAT_VERSION}-linux-x86_64.tar.gz -o /tmp/fb.tar.gz && \
    tar zxf /tmp/fb.tar.gz -C /opt && \
    mv /opt/filebeat-${FILEBEAT_VERSION}-linux-x86_64 /opt/filebeat && \
    rm -rf /tmp/*

ADD ./logs_input_txt/ /problem4/input/
ADD ./filebeat-apache.yml /problem4/filebeat.yml

ENV PATH=/opt/filebeat:$PATH

WORKDIR /problem4
