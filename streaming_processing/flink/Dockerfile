# Flink cluster image with PyFlink installed, so Python jobs can run on the
# JobManager/TaskManager (the official flink image ships without Python).
# Flink 2.0 matches the Kafka connector jar: flink-sql-connector-kafka-4.0.1-2.0.jar
FROM flink:2.0

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Install the PyFlink version that exactly matches the Flink distribution in the image
RUN FLINK_VERSION=$(ls /opt/flink/lib/flink-dist-*.jar | sed -E 's/.*flink-dist-(.*)\.jar/\1/') \
    && echo "Installing apache-flink==${FLINK_VERSION}" \
    && pip3 install --no-cache-dir "apache-flink==${FLINK_VERSION}" kafka-python

# Kafka connector available on the classpath of every JobManager/TaskManager
COPY jars/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/lib/
