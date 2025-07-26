import json
from confluent_kafka import Consumer
import mysql.connector


class SyncServiceMySQL:
    """Consume Neo4j change events and write them into MySQL tables.

    Events should have:
      - eventId
      - payload: dict with columns
      - source: origin system
      - table: destination table name
    """

    def __init__(self, kafka_config, mysql_config, topic, source_label="NEO4J"):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])
        self.conn = mysql.connector.connect(**mysql_config)
        self.source_label = source_label
        self.seen_event_ids = set()

    def close(self):
        self.consumer.close()
        self.conn.close()

    def handle_event(self, event):
        if not event:
            return
        try:
            data = json.loads(event.value())
        except json.JSONDecodeError:
            print("invalid event", event.value())
            return

        event_id = data.get("eventId")
        if event_id in self.seen_event_ids:
            return
        self.seen_event_ids.add(event_id)

        if data.get("source") == self.source_label:
            return

        table = data.get("table")
        payload = data.get("payload", {})
        if not table or "id" not in payload:
            print("missing table or id")
            return

        columns = ", ".join(payload.keys())
        placeholders = ", ".join(["%s"] * len(payload))
        updates = ", ".join(f"{c}=VALUES({c})" for c in payload.keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        with self.conn.cursor() as cur:
            cur.execute(sql, list(payload.values()))
        self.conn.commit()

    def start(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("consumer error", msg.error())
                    continue
                self.handle_event(msg)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()


if __name__ == "__main__":
    kafka_conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "sync-service-mysql",
        "auto.offset.reset": "earliest",
    }
    mysql_conf = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": "testdb",
    }
    service = SyncServiceMySQL(kafka_conf, mysql_conf, "neo4j.customer.v1")
    service.start()
