FROM python:3.9-slim-buster

RUN apt-get update && \
    apt-get -y install cron busybox
    
RUN printenv | sed 's/^\(.*\)$/export \1/g' > /root/project_env.sh

COPY requirements.txt /

RUN pip install -r /requirements.txt

COPY src/ /app
RUN chmod +x /app/main.py

COPY cronpy /tmp/root.crontab
RUN crontab /tmp/root.crontab

COPY run_python.sh /app/run_python.sh
RUN chmod +x /app/run_python.sh

CMD busybox syslogd -C; cron -L 2 -f