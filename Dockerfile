FROM python:3.13.8-slim-bookworm
# default shell is sh
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk curl
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install pyspark==4.1.1
ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}
# Fetch spark version 4.1.1 and download the appropriate binary file
RUN curl -O https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
 # Unpack the file and cleanup the binary file
RUN tar xvzf spark-4.1.1-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-4.1.1-bin-hadoop3.tgz

RUN curl -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    
# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"
COPY ./spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"
ENTRYPOINT ["/bin/bash"]