FROM python:3.9-slim-buster

# Install Python Requirements
COPY requirements.txt /
RUN pip install -r /requirements.txt

# Copy python source code
COPY src/ /app

WORKDIR /app

CMD [ "python3", "-u", "main.py" ]
