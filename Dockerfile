FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
 && mkdir -p /opt/jars \
 && curl -fsSL -o /opt/jars/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
ENV JAVA_HOME=/opt/java

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
