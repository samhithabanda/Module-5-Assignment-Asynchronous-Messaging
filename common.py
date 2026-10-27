"""Shared settings and helpers for the order producer and worker."""

import logging
import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

QUEUE_NAME = os.getenv("ORDER_QUEUE", "orders")


def get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
    )
    return pika.BlockingConnection(params)


def declare_queue(channel) -> None:
    # Durable queue so messages survive a broker restart.
    channel.queue_declare(queue=QUEUE_NAME, durable=True)


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    return logging.getLogger(name)
