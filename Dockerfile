FROM python:3.9-slim

RUN apt-get update && \
    apt-get -y install cron

COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY src/ /app
COPY cronpy /var/spool/cron/crontabs/cronpy

# CMD crond -l 2 -f