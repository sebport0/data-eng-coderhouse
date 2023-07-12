FROM apache/airflow:2.6.2-python3.9

USER root

# Install OpenJDK-11
RUN apt-get update && \
  apt-get install -y openjdk-11-jdk && \
  apt-get install -y ant && \
  apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
