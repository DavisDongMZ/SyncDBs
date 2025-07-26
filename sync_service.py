import json
from confluent_kafka import Consumer
from neo4j import GraphDatabase


class SyncService:
    """Consume change events from Kafka and upsert them into Neo4j.

    Events are expected to be JSON with at least:
      - ``eventId``: unique identifier for idempotency
      - ``schemaVersion``: integer version for routing
      - ``payload``: dictionary describing properties
      - ``source``: origin system (e.g. ``MYSQL`` or ``NEO4J``)
      - ``label``: Neo4j node label

    Different ``schemaVersion`` values can be routed to custom handlers to
    support schema evolution.
    """

    def __init__(self, kafka_config, neo4j_uri, neo4j_auth, topic,
                 source_label="MYSQL", version_handlers=None):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([topic])
        self.driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        self.source_label = source_label
        self.seen_event_ids = set()
        self.version_handlers = version_handlers or {
            1: self._handle_v1,
            2: self._handle_v2,
        }

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

        version = data.get("schemaVersion", 1)
        handler = self.version_handlers.get(version)
        if not handler:
            print(f"no handler for schemaVersion {version}")
            return

        handler(data)

    def _handle_v1(self, data):
        """Default handler for schemaVersion 1 events (node upserts)."""
        label = data.get("label", "Entity")
        props = data.get("payload", {})
        key = props.get("id")
        if key is None:
            print("missing id in payload")
            return

        with self.driver.session() as session:
            session.execute_write(self._merge_node, label, key, props)

    def _handle_v2(self, data):
        """Example handler for relationship events."""
        start = data.get("start") or {}
        end = data.get("end") or {}
        rtype = data.get("relationshipType")
        if not start.get("id") or not end.get("id") or not rtype:
            print("incomplete relationship event")
            return

        props = data.get("payload", {})
        with self.driver.session() as session:
            session.execute_write(
                self._merge_relationship,
                start.get("label", "Entity"),
                start["id"],
                end.get("label", "Entity"),
                end["id"],
                rtype,
                props,
            )

    @staticmethod
    def _merge_node(tx, label, key, props):
        prop_keys = ", ".join(f"{k}: ${k}" for k in props.keys())
        query = f"MERGE (n:{label} {{id: $id}}) SET n += {{{prop_keys}}}"
        tx.run(query, **props)

    @staticmethod
    def _merge_relationship(tx, start_label, start_id, end_label, end_id, rtype, props):
        prop_clause = "" if not props else " SET r += $props"
        query = (
            f"MERGE (a:{start_label} {{id: $start_id}}) "
            f"MERGE (b:{end_label} {{id: $end_id}}) "
            f"MERGE (a)-[r:{rtype}]->(b)" + prop_clause
        )
        parameters = {
            "start_id": start_id,
            "end_id": end_id,
        }
        if props:
            parameters["props"] = props
        tx.run(query, **parameters)

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
