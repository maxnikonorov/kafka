import json
import time
import psycopg2
from kafka import KafkaProducer

# Инициализация продюсера Kafka
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(
        v, 
        default=lambda o: o.isoformat() if hasattr(o, 'isoformat') else str(o)
    ).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

# Добавим колонку sent-to-kafka в таблицу user-logins и поумолчанию флаг false
cursor.execute("""
        ALTER TABLE user_logins 
        ADD COLUMN IF NOT EXISTS sent_to_kafka BOOLEAN DEFAULT FALSE;
        
""")
conn.commit()

print("Продюсер запущен. Проверяем новые события в таблице user_logins...")

while True:
    cursor.execute("""
        SELECT id, username, event_type, event_time 
        FROM user_logins 
        WHERE sent_to_kafka = FALSE
        LIMIT 100
    """)
    rows = cursor.fetchall()

    if rows:
        sent_ids = []
        for row in rows:
            row_id, username, event_type, event_time = row
            
            message = {
                "id": row_id,
                "user": username,
                "event": event_type,
                "timestamp": event_time
            }
            
            producer.send('user_events', message)
            sent_ids.append(row_id)
            print(f" [Kafka] Отправлено событие ID {row_id} из user_logins")
            
        producer.flush()

        cursor.execute("""
            UPDATE user_logins 
            SET sent_to_kafka = TRUE 
            WHERE id = ANY(%s)
        """, (sent_ids,))
        conn.commit()

    time.sleep(2)
