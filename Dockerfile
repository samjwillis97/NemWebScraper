FROM python:3.9-slim-buster

RUN apt-get update && \
    apt-get -y install cron busybox

COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY src/ /app
RUN chmod +x /app/main.py

COPY cronpy /tmp/root.crontab
RUN crontab /tmp/root.crontab

CMD busybox syslogd -C; cron -L 2 -f