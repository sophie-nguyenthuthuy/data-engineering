"""Ad-hoc Dagster job that replays DLQ messages back onto the source topic.

The DLQ holds Avro-encoded records that failed the sink connector after the
5-minute retry window (see `docs/RELIABILITY.md`). After the downstream issue
is fixed, an operator launches this job from the Dagster UI to rebroadcast
them.

Running it is safe as long as the sink connector is healthy again:
`ReplacingMergeTree` on `user_interactions` dedups on merge, and the sink's
`exactlyOnce=true` blocks repeat inserts of the same Kafka offset — but neither
of those guards deduplicates *different* messages with the same business key
(event_id). That's what this job explicitly expects the operator to have
reasoned about.
"""

from dagster import Config, OpExecutionContext, Output, job, op

from orchestrator.resources import KafkaResource


class DLQReplayConfig(Config):
    max_messages: int = 100
    group_id: str = "dlq-replay"


@op
def replay_dlq(
    context: OpExecutionContext,
    config: DLQReplayConfig,
    kafka: KafkaResource,
) -> Output[dict]:
    from confluent_kafka import Consumer, Producer  # runtime import — big dep

    base = kafka.client_config()
    consumer = Consumer(
        {
            **base,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({**base, "enable.idempotence": True, "acks": "all"})

    consumer.subscribe([kafka.dlq_topic])
    replayed = 0
    poll_errors: list[str] = []

    try:
        while replayed < config.max_messages:
            msg = consumer.poll(5.0)
            if msg is None:
                break  # idle → nothing more to replay right now
            if msg.error():
                poll_errors.append(str(msg.error()))
                continue
            producer.produce(kafka.source_topic, key=msg.key(), value=msg.value())
            producer.poll(0)
            replayed += 1
        producer.flush(timeout=30)
        if replayed > 0:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    context.log.info(
        "DLQ replay complete: replayed=%d poll_errors=%d", replayed, len(poll_errors)
    )
    return Output(
        {
            "replayed": replayed,
            "poll_errors": poll_errors,
            "dlq_topic": kafka.dlq_topic,
            "target_topic": kafka.source_topic,
        }
    )


@job(description="Move messages from the DLQ back onto the source topic.")
def dlq_replay_job() -> None:
    replay_dlq()
