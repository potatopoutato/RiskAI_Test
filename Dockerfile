FROM openjdk:8-jdk

RUN apt-get update && apt-get install -y curl

# Download and install Apache Spark
RUN curl -O https://downloads.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz && \
    tar -xvf spark-3.2.4-bin-hadoop3.2.tgz && \
    mv spark-3.2.4-bin-hadoop3.2 /spark && \
    rm spark-3.2.4-bin-hadoop3.2.tgz

# Set environment variables
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Start the Spark shell
CMD spark-shell --master local[*]

COPY ./data /data
