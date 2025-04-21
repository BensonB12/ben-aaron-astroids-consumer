import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    ConsumerOffsetSpecification,
    OffsetType,
)

STREAM_NAME = "hello-python-stream"
# 5GB
STREAM_RETENTION = 5000000000


async def on_message(msg: AMQPMessage, message_context: MessageContext):
    stream = message_context.consumer.get_stream(message_context.subscriber_name)
    print("Got message: {} from stream {}".format(msg, stream))


async def main():
    consumer = Consumer(host="localhost", username="guest", password="guest")
    await consumer.create_stream(
        STREAM_NAME, exists_ok=True, arguments={"MaxLengthBytes": STREAM_RETENTION}
    )

    await consumer.start()
    await consumer.subscribe(
        stream=STREAM_NAME,
        callback=on_message,
        offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
    )
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
