FROM python:3.9-slim-buster

# Install Cron and Busybox for Logs
RUN apt-get update && \
    apt-get -y install cron busybox

# Install Python Requirements
COPY requirements.txt /
RUN pip install -r /requirements.txt

# Copy python source code
COPY src/ /app
RUN chmod +x /app/main.py

# Setup Cron Job
COPY cronpy /tmp/root.crontab
RUN crontab /tmp/root.crontab

# Copy Shell script for running Python
COPY run_python.sh /run_python.sh
RUN chmod +x /run_python.sh

# Copy Entrypoint Shell Script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD /entrypoint.sh