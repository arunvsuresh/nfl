# FROM apache/airflow:2.2.0
# COPY requirements.txt /
# RUN pip install --no-cache-dir -r /requirements.txt

FROM prefecthq/prefect:2.7.7-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir


RUN mkdir -p /opt/prefect/data/
RUN mkdir -p /opt/prefect/flows/

COPY nfl_prefect/flows/ /opt/prefect/flows