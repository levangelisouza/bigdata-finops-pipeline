from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
from datetime import datetime
import random
import os
import sys

TOPICO = 'cotacoes-acoes'
BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')  # Dentro do Docker

TICKERS = ['VALE3', 'PETR4', 'ITUB4', 'MGLU3', 'BBDC4']

# Tenta conectar até o Kafka ficar disponível
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[OK] Conectado ao broker Kafka em {BROKER}")
        break
    except NoBrokersAvailable:
        print(f"[AGUARDANDO] Kafka não disponível em {BROKER}, tentando novamente em 3s...")
        time.sleep(3)

print(f"Iniciando publicação no tópico '{TOPICO}'...")

while True:
    msg = {
        "ticker": random.choice(TICKERS),
        "preco": round(random.uniform(10, 100), 2),
        "data": datetime.utcnow().isoformat()
    }
    try:
        producer.send(TOPICO, msg)
        print("Mensagem enviada:", msg)
    except Exception as e:
        print("Erro ao enviar mensagem:", e, file=sys.stderr)
    time.sleep(2)

