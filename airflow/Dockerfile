FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y python3.8 python3-pip libmysqlclient-dev
RUN mkdir /opt/airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

RUN pip3 install apache-airflow==2.2.1 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.2.1/constraints-3.8.txt
RUN pip3 install pandas beautifulsoup4 sklearn
RUN pip3 install apache-airflow-providers-mysql==2.1.1


RUN airflow db init
RUN airflow users create \
    --username admin \
    --password password \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

#------------
RUN pip3 install selenium && \
    pip3 install bs4 && \
    pip3 install elasticsearch && \
    pip3 install lxml && \
    pip3 install beautifulsoup4
#------------
RUN pip3 install xmltodict