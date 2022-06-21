FROM python:3.10-slim-buster

WORKDIR /pipeline

COPY ./requirements.txt .

# Install psycopg2 and pipeline dependencies
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

COPY ./src .

CMD ["python", "main.py"]
