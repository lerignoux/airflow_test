FROM ubuntu:15.10

# Install everything needed
RUN apt-get update \
  && apt-get install cython python-pip python-dev vim -y\
  && apt-get autoremove -y \
  && apt-get clean

# Default nyuki's api port
EXPOSE 8080

# Triggers
WORKDIR /home/
ADD ./ ./
RUN pip install -r requirements.txt
RUN airflow initdb

ENV AIRFLOW_HOME ~/airflow
ENV CMD airflow webserver -p 8080

# Airflow entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

