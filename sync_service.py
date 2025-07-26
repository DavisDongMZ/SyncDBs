import json
from confluent_kafka import Consumer
from neo4j import GraphDatabase


class SyncService:
    """Consume change events from Kafka and upsert them into Neo4j.

    Events are expected to be JSON with at least:
      - eventId: unique identifier for idempotency
      - payload: dictionary describing node properties
      - source: origin system (e.g. 'MYSQL', 'NEO4J')
      - label: Neo4j node label
    """

    def __init__(self, kafka_config, neo4j_uri, neo4j_auth, topic, source_label="MYSQL"):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])
        self.driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        self.source_label = source_label
        self.seen_event_ids = set()

    def close(self):
        self.consumer.close()
        self.driver.close()

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
            # deduplicate
            return
        self.seen_event_ids.add(event_id)

        if data.get("source") == self.source_label:
            # drop events originating from the same system to avoid loop
            return

        label = data.get("label", "Entity")
        props = data.get("payload", {})
        key = props.get("id")
        if key is None:
            print("missing id in payload")
            return

        with self.driver.session() as session:
            session.execute_write(self._merge_node, label, key, props)

    @staticmethod
    def _merge_node(tx, label, key, props):
        prop_keys = ", ".join(f"{k}: ${k}" for k in props.keys())
        query = f"MERGE (n:{label} {{id: $id}}) SET n += {{{prop_keys}}}"
        tx.run(query, **props)

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
        "group.id": "sync-service",
        "auto.offset.reset": "earliest",
    }
    service = SyncService(kafka_conf, "bolt://localhost:7687", ("neo4j", "test"), "mysql.customer.v1")
    service.start()
