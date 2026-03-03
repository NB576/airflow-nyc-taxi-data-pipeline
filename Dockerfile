FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:13.4.0-base

USER root

# Install Java 17 
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Spark setup
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xzC /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"
ENV SPARK_HOME="/opt/spark"

RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# -base images require you to explicitly call Astro's install scripts for requirements.txt + packages.txt.
COPY requirements.txt .
RUN /usr/local/bin/install-python-dependencies

COPY packages.txt .        
RUN /usr/local/bin/install-system-packages || true

RUN pip install pyspark==3.5.3

USER astro


