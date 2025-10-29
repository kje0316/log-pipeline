# 1. 님이 보여주신 yml의 Airflow 이미지를 기반으로 합니다.
FROM apache/airflow:2.9.2

# 2. 루트 권한으로 Spark 설치에 필요한 Java와 wget 설치
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean

# 3. Spark 3.4.4 (Hadoop 3 포함) 다운로드 및 설치 - Java 17 지원
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvfz spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 4. 다시 Airflow 유저로 돌아와서 PySpark 및 드라이버 설치
USER airflow
RUN pip install --no-cache-dir pyspark==3.4.4 kafka-python boto3 psycopg2-binary