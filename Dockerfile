FROM python:3.12-slim-bookworm

RUN apt-get clean && \
    apt-get update && \
    apt-get install -y --no-install-recommends wget gnupg2 openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=4.1.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN pip install --no-cache-dir --upgrade pip

RUN wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.10.2.jre11/mssql-jdbc-12.10.2.jre11.jar -P /lib

COPY src ./src
COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

CMD ["python", "src/main.py"]