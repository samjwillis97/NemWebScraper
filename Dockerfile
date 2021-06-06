FROM python:3.9

COPY requirements.txt /

RUN pip install -r /requirements.txt
RUN apt-get install -y cron

COPY src/ /app
COPY cronpy /var/spool/cron/crontabs/cronpy

CMD crond -l 2 -f