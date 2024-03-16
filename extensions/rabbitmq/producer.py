import aio_pika
import asyncio


class AsyncRabbitMQProducer:
    """
    Asynchronous RabbitMQ message producer.

    This class allows sending messages to a RabbitMQ queue asynchronously.

    Parameters
    ----------
    url : str
        The URL of the RabbitMQ server.

    Raises
    ------
    ValueError
        If the URL does not start with 'amqp://'.

    """

    def __init__(self, url):
        if not url.startswith('amqp://'):
            raise ValueError("URL must start with 'amqp://'")

        self.url = url
        self.connection = None
        self.channel = None

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

    async def publish(self, queue_name, message, **kwargs):
        """
        Publish a message to the specified queue asynchronously.

        Parameters
        ----------
        queue_name : str
            The name of the queue to publish the message to.
        message : str
            The message to be published.
        **kwargs
            Additional arguments to be passed to the `publish` method.

        """
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=queue_name,
            **kwargs
        )

    async def __aenter__(self):
        """
        Asynchronous context manager entry.

        Returns
        -------
        AsyncRabbitMQProducer
            The producer instance.

        """
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Asynchronous context manager exit.

        Parameters
        ----------
        exc_type : type
            The type of the exception.
        exc : Exception
            The exception instance.
        tb : traceback
            The traceback object.

        """
        await self.close()

    def __enter__(self):
        """
        Raise an error when attempting to use synchronous context manager.

        Raises
        ------
        RuntimeError
            Always raised to indicate that AsyncRabbitMQProducer must be used as an asynchronous context manager.

        """
        raise RuntimeError("AsyncRabbitMQProducer must be used as an asynchronous context manager")

    def __exit__(self, exc_type, exc, tb):
        """
        Dummy method for synchronous context manager.

        Parameters
        ----------
        exc_type : type
            The type of the exception.
        exc : Exception
            The exception instance.
        tb : traceback
            The traceback object.

        """
        pass


if __name__ == '__main__':
    async def main():
        test_message = 'test message'
        async with AsyncRabbitMQProducer('amqp://guest:guest@localhost/') as producer:
            await producer.publish('test_queue', test_message)


    asyncio.run(main())
