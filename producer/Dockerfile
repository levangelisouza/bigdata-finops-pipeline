FROM python:3.8-slim

WORKDIR /app

COPY producer.py .

RUN pip install kafka-python

CMD ["python", "producer.py"]

