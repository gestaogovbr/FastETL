FROM apache/airflow:2.5.0-python3.9

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         unixodbc-dev \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add --no-tty - \
  && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update -yqq \
  && ACCEPT_EULA=Y apt-get install -yqq msodbcsql17 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean

COPY . /opt/airflow/fastetl

RUN chown -R airflow /opt/airflow/fastetl

USER airflow

RUN pip install --no-cache-dir --user \
    apache-airflow[microsoft.mssql,odbc,samba] \
    apache-airflow-providers-common-sql \
    pytest==6.2.5 \
    /opt/airflow/fastetl \
    && rm -rf /opt/airflow/fastetl

RUN airflow db init

ENTRYPOINT airflow scheduler

EXPOSE 8080
