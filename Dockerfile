FROM apache/airflow:2.0.1
#from https://github.com/bitnami/bitnami-docker-airflow/issues/16
COPY install_pkgs/oracle-instantclient19.10-basiclite-19.10.0.0.0-1.x86_64.rpm /tmp/oracle-instantclient19.10-basiclite-19.10.0.0.0-1.x86_64.rpm
ARG SSL_KEYSTORE_PASSWORD
USER root
RUN apt-get update && apt-get install -y alien
RUN alien -i /tmp/oracle-instantclient19.10-basiclite-19.10.0.0.0-1.x86_64.rpm
RUN apt-get install libaio1
USER ${AIRFLOW_UID:-50000}
ENV LD_LIBRARY_PATH /usr/lib/oracle/19.10/client64/lib/${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
ENV ORACLE_HOME /usr/lib/oracle/19.10/client64
ENV PATH $PATH:$ORACLE_HOME/bin
#RUN ln -s /usr/include/oracle/19.10/client64 $ORACLE_HOME/include
RUN pip install --no-cache-dir --user cx_Oracle
RUN pip install --no-cache-dir --user apache-airflow-providers-oracle
RUN pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

#https://stackoverflow.com/questions/28372328/how-to-install-the-google-cloud-sdk-in-a-docker-image
RUN curl -sSL https://sdk.cloud.google.com | bash

ADD install_pkgs/OpenJDK8U-jre_x64_linux_hotspot_8u292b10.tar.gz /home/airflow

# Setup JAVA_HOME, this is useful for docker commandline
ENV JAVA_HOME /home/airflow/jdk8u292-b10-jre
ENV PATH $PATH:/home/airflow/google-cloud-sdk/bin:$JAVA_HOME/bin

