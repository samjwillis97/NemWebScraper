FROM python:3.9-alpine

COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY src/ /app
COPY cronpy /var/spool/cron/crontabs/cronpy

CMD crond -l 2 -f