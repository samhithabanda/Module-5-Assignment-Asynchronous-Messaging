"""Order Producer: publishes order messages to the RabbitMQ `orders` queue.

Usage:
    python producer.py                     # publish 10 orders
    python producer.py --count 20          # publish 20 orders
    python producer.py --count 5 --duplicate
        # publish 5 orders, then re-publish the first one with the SAME event_id
"""

import argparse
import json
import random
import uuid

import pika

from common import QUEUE_NAME, declare_queue, get_connection, setup_logging

log = setup_logging("producer")


def build_order(n: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{n:04d}",
        "item_id": f"ITEM-{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
    }


def publish(channel, order: dict) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=order["event_id"],
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=10, help="number of orders to publish")
    parser.add_argument("--start", type=int, default=1, help="first order number")
    parser.add_argument(
        "--duplicate",
        action="store_true",
        help="re-publish the first order again with the same event_id",
    )
    args = parser.parse_args()

    connection = get_connection()
    channel = connection.channel()
    declare_queue(channel)
    channel.confirm_delivery()

    orders = [build_order(n) for n in range(args.start, args.start + args.count)]
    for order in orders:
        publish(channel, order)
        log.info("Published %s", json.dumps(order))

    if args.duplicate and orders:
        dup = orders[0]
        publish(channel, dup)
        log.info("Published DUPLICATE %s", json.dumps(dup))

    connection.close()
    log.info("Done. %d message(s) sent to queue '%s'.",
             len(orders) + (1 if args.duplicate and orders else 0), QUEUE_NAME)


if __name__ == "__main__":
    main()
