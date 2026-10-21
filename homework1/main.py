import logging
import os
import threading
import time
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
    StringDeserializer,
    StringSerializer,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# Адреса берутся из окружения (в docker-compose заданы внутренние).
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
assert BOOTSTRAP_SERVERS, "set env variable BOOTSTRAP_SERVERS"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
assert SCHEMA_REGISTRY_URL, "set env variable SCHEMA_REGISTRY_URL"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

schema = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Message",
    "type": "object",
    "properties": {"id": {"type": "string"}, "text": {"type": "string"}},
    "required": ["id", "text"]
}
"""


key_serializer = StringSerializer("utf_8")
key_deserializer = StringDeserializer("utf_8")
value_serializer = JSONSerializer(
    schema_str=schema,
    schema_registry_client=schema_registry_client,
)
value_deserializer = JSONDeserializer(
    schema_str=schema,
    schema_registry_client=schema_registry_client,
)


class MessageProducer:
    """Продюсер: раз в 3 секунды отправляет JSON-сообщение в топик."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.producer = SerializingProducer(
            {
                "bootstrap.servers": bootstrap_servers,  # адреса брокеров для первого подключения
                "enable.idempotence": True,  # исключает дубли при повторной отправке
                "retries": 5,  # число повторов отправки при временной ошибке
                "retry.backoff.ms": 500,  # пауза между повторами, мс
                "delivery.timeout.ms": 60000,  # общий лимит на доставку, включая повторы, мс
                "key.serializer": key_serializer,  # ключ: строка -> байты
                "value.serializer": value_serializer,  # значение: dict -> JSON по схеме из Schema Registry
                "acks": "all",  # лидер ждёт подтверждения от всех синхронных реплик: сообщение не потеряется при сбое брокера (At Least Once)
            }
        )
        self.topic = topic
        self._running = False
        self._logger = logging.getLogger(type(self).__name__)

    def send(self, num: int) -> None:
        key = f"key-{num}"
        value = {"id": str(uuid4()), "text": f"This is a new message #{num}!"}
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                on_delivery=self._delivery_report,
            )
            self.producer.flush()
        except SerializationError as exc:
            self._logger.error(
                "Ошибка при сериализации сообщения: %s [topic=%s, key=%s, value=%s]",
                str(exc),
                self.topic,
                key,
                value,
            )
        except Exception as exc:
            self._logger.error(
                "Ошибка при отправке сообщения: %s [topic=%s, key=%s, value=%s]",
                str(exc),
                self.topic,
                key,
                value,
            )

    def _delivery_report(self, err, message):
        if err is not None:
            self._logger.error(
                "Сообщение не доставлено: %s [key=%s, topic=%s, partition=%s]",
                err,
                message.key(),
                message.topic(),
                message.partition(),
            )
            return
        detail = (
            key_deserializer(
                message.key(),
                SerializationContext(message.topic(), MessageField.KEY),
            ),
            value_deserializer(
                message.value(),
                SerializationContext(message.topic(), MessageField.VALUE),
            ),
            message.topic(),
            message.partition(),
        )
        self._logger.info(
            "Сообщение доставлено [key=%s, value=%s, topic=%s, partition=%s]",
            *detail,
        )

    def run(self) -> None:
        self._running = True
        try:
            i = 0
            while self._running:
                self.send(i)
                i += 1
                time.sleep(3.0)

        finally:
            self.producer.close()

    def stop(self) -> None:
        self._running = False


class SingleMessageConsumer:
    """Консьюмер, читающий по одному сообщению с автокоммитом offset."""

    def __init__(self, bootstrap_servers: str, topics: list[str], group_id: str) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "auto.offset.reset": "earliest",  # без сохранённого offset читать с начала топика
                "enable.auto.commit": True,  # True: offset коммитится автоматически; False: вручную после обработки
                "group.id": group_id,  # группа консьюмеров, между участниками которой делятся партиции
            }
        )
        self.topics = topics
        self._running = False
        self._logger = logging.getLogger(type(self).__name__)

    def read(self) -> None:
        message = self.consumer.poll(1.0)
        if message is None:
            return

        detail = (message.topic(), message.partition(), message.offset())
        if err := message.error():
            self._logger.error(
                "Произошла ошибка: %s [topic=%s, partition=%s, offset=%s]",
                err,
                *detail,
            )
            return

        try:
            key = key_deserializer(
                message.key(),
                SerializationContext(message.topic(), MessageField.KEY),
            )
            value = value_deserializer(
                message.value(),
                SerializationContext(message.topic(), MessageField.VALUE),
            )
        except SerializationError as exc:
            self._logger.error(
                "Ошибка при десериализации сообщения: %s [topic=%s, partition=%s, offset=%s]",
                str(exc),
                *detail,
            )
            return

        decoded_key = key if key is not None else "<no-key>"
        decoded_value = value if value is not None else "<no-value>"
        self._logger.info(
            "Получено сообщение: key=%s, value=%s [topic=%s, partition=%s, offset=%s]",
            decoded_key,
            decoded_value,
            *detail,
        )

    def run(self) -> None:
        self._running = True
        self.consumer.subscribe(self.topics)
        try:
            while self._running:
                self.read()
        finally:
            self.consumer.close()

    def stop(self) -> None:
        self._running = False


class BatchMessageConsumer:
    """Консьюмер, накапливающий пачку сообщений и коммитящий offset вручную."""

    def __init__(
        self,
        bootstrap_servers: str,
        topics: list[str],
        group_id: str,
        batch_size: int = 10,
        fetch_min_bytes: int = 1300,  # (key ~5 bytes + value ~115 bytes + record_overhead ~10 bytes) * 10
    ) -> None:
        self.batch: list[Any] = []
        self.batch_size = batch_size
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "fetch.min.bytes": fetch_min_bytes,  # брокер отвечает, когда накоплено не меньше байт
                "group.id": group_id,
                "fetch.wait.max.ms": 5000,  # максимум ожидания fetch.min.bytes, мс
            }
        )
        self.topics = topics
        self._running = False
        self._logger = logging.getLogger(type(self).__name__)

    def read_batch(self) -> None:
        messages = self.consumer.consume(num_messages=self.batch_size, timeout=1.0)
        for message in messages:
            detail = (message.topic(), message.partition(), message.offset())
            if err := message.error():
                self._logger.error(
                    "Произошла ошибка: %s [topic=%s, partition=%s, offset=%s]",
                    err,
                    *detail,
                )
                continue

            try:
                key = key_deserializer(
                    message.key(),
                    SerializationContext(message.topic(), MessageField.KEY),
                )
                value = value_deserializer(
                    message.value(),
                    SerializationContext(message.topic(), MessageField.VALUE),
                )
            except SerializationError as exc:
                self._logger.error(
                    "Ошибка при десериализации сообщения: %s [topic=%s, partition=%s, offset=%s]",
                    str(exc),
                    *detail,
                )
                continue

            decoded_key = key if key is not None else "<no-key>"
            decoded_value = value if value is not None else "<no-value>"
            self.batch.append((decoded_key, decoded_value, *detail))

        if len(self.batch) >= self.batch_size:
            self._logger.info(
                "Получена пачка из %d сообщений: %s",
                len(self.batch),
                "; ".join(
                    f"key={key}, value={value} [topic={topic}, partition={partition}, offset={offset}]"
                    for key, value, topic, partition, offset in self.batch
                ),
            )
            self.consumer.commit(asynchronous=False)
            self.batch.clear()

    def run(self) -> None:
        self._running = True
        self.consumer.subscribe(self.topics)
        try:
            while self._running:
                self.read_batch()
        finally:
            self.consumer.close()

    def stop(self) -> None:
        self._running = False


if __name__ == "__main__":
    workers: tuple[MessageProducer, SingleMessageConsumer, BatchMessageConsumer] = (
        MessageProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic="my-topic",
        ),
        SingleMessageConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=["my-topic"],
            group_id="single-group",
        ),
        BatchMessageConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topics=["my-topic"],
            group_id="batch-group",
        ),
    )
    threads = [threading.Thread(target=w.run, name=type(w).__name__) for w in workers]
    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.getLogger("main").info("Останавливаю...")
    finally:
        for w in workers:
            w.stop()
        for t in threads:
            t.join()
