import asyncio

import aio_pika

class AsyncRabbitMQConsumer:
    """A consumer class for asynchronous RabbitMQ message consumption."""

    def __init__(self, url):
        """
        Initialize the AsyncRabbitMQConsumer.

        Parameters:
        - url (str): The URL of the RabbitMQ server.

        """
        self.url = url
        self.connection = None
        self.channel = None
        self._consume_flag = True  # Flag to control consumption

    async def connect(self):
        """
        Connect to the RabbitMQ server asynchronously.
        """
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()

    async def close(self):
        """
        Close the connection to the RabbitMQ server asynchronously.
        """
        await self.channel.close()
        await self.connection.close()

    async def consume(self, queue_name, callback):
        """
        Consume messages from the specified queue asynchronously.

        Parameters:
        - queue_name (str): The name of the queue to consume messages from.
        - callback (callable): A callback function to process each received message.
                               The function should accept a single argument, which is
                               the decoded message body.

        """
        queue = await self.channel.declare_queue(queue_name)
        async with queue.iterator() as queue_iter:
            while self._consume_flag:
                async for message in queue_iter:
                    async with message.process():
                        await callback(message.body.decode())
        await self.close()

    async def stop(self):
        """
        Stop consuming messages asynchronously.
        """
        self._consume_flag = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __enter__(self):
        raise RuntimeError("AsyncRabbitMQProducer must be used as an asynchronous context manager")

    def __exit__(self, exc_type, exc, tb):
        pass

if __name__ == '__main__':
    async def main():
        async def callback(message):
            print(message)
        async with AsyncRabbitMQConsumer('amqp://guest:guest@localhost/') as consumer:
            await consumer.consume('test_queue', callback=callback)

    asyncio.run(main())