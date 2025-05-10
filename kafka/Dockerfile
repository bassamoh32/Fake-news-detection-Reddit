FROM python:3.11-slim

WORKDIR /app
COPY . /app/

RUN pip install --no-cache-dir \
    praw==7.8.1 \
    kafka-python==2.0.2 \
    python-dotenv==0.19.2 \
    PyYAML==6.0.2

CMD ["python", "kafka/producer.py"]