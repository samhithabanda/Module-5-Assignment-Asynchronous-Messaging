"""Order Worker: consumes order messages, processes them, then ACKs.

Run two or more of these at once to get competing consumers.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2

Failure simulation (worker exits BEFORE sending the ACK):
    python worker.py --name worker-1 --crash-on ORD-0003
    python worker.py --name worker-1 --crash-on ORD-0003 --crash-mode after-process
"""

import argparse
import fcntl
import json
import os
import time

from common import QUEUE_NAME, declare_queue, get_connection, setup_logging

PROCESSED_FILE = os.getenv(
    "PROCESSED_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "processed_events.txt")
)


class ProcessedEvents:
    """File-backed set of processed event_ids, shared by all workers.

    A shared file (instead of a per-process set) means a duplicate is caught
    no matter which worker receives it. flock keeps concurrent writes safe.
    """

    def __init__(self, path: str):
        self.path = path
        open(self.path, "a").close()

    def contains(self, event_id: str) -> bool:
        with open(self.path, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return event_id in (line.strip() for line in f)

    def add(self, event_id: str) -> None:
        with open(self.path, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(event_id + "\n")
            f.flush()
            os.fsync(f.fileno())


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and process order messages")
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds of simulated work per order")
    parser.add_argument("--crash-on", help="order_id that makes this worker exit before ACKing")
    parser.add_argument(
        "--crash-mode",
        choices=["before-process", "after-process"],
        default="before-process",
        help="crash before the business action, or after it but still before the ACK",
    )
    args = parser.parse_args()

    log = setup_logging(args.name)
    processed = ProcessedEvents(PROCESSED_FILE)

    connection = get_connection()
    channel = connection.channel()
    declare_queue(channel)
    # Only one unacked message per worker at a time -> fair round-robin dispatch.
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        order = json.loads(body)
        event_id = order["event_id"]
        tag = " (REDELIVERED)" if method.redelivered else ""
        log.info("Received order %s event_id=%s%s", order["order_id"], event_id, tag)

        if processed.contains(event_id):
            log.info("DUPLICATE detected for event_id=%s (order %s) - skipping business action",
                     event_id, order["order_id"])
            ch.basic_ack(delivery_tag=method.delivery_tag)
            log.info("ACK sent for duplicate %s", order["order_id"])
            return

        crashing = args.crash_on and order["order_id"] == args.crash_on

        if crashing and args.crash_mode == "before-process":
            log.warning("SIMULATED CRASH while processing %s - exiting WITHOUT ACK", order["order_id"])
            os._exit(1)

        log.info("Processing order %s: item=%s quantity=%s",
                 order["order_id"], order["item_id"], order["quantity"])
        time.sleep(args.delay)  # simulated business work (e.g. reserve inventory)
        processed.add(event_id)
        log.info("Finished order %s", order["order_id"])

        if crashing and args.crash_mode == "after-process":
            log.warning("SIMULATED CRASH after processing %s but BEFORE ACK - exiting", order["order_id"])
            os._exit(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("ACK sent for %s", order["order_id"])

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Waiting for messages on queue '%s'. Press CTRL+C to exit.", QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Shutting down")
        channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
