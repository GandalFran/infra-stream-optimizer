FROM apache/flink:1.17.0-java11

# Install Python 3.8+
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev build-essential && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/flink

COPY requirements.txt .
RUN pip3 install --upgrade pip && pip3 install wheel && pip3 install -r requirements.txt

# Download Kafka Connector
RUN mkdir -p /opt/flink/lib && \
    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar

COPY src/ /opt/flink/usrlib/

# Command to run the job (placeholder, normally via jobmanager)
CMD ["taskmanager"]
