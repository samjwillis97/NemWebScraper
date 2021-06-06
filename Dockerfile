FROM python:3.9-slim-buster

RUN apt-get update && \
    apt-get -y install cron busybox

COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY src/ /app
COPY cronpy /var/spool/cron/crontabs/root

CMD busybox syslogd -C; cron -L 2 -f