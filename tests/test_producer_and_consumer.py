import asyncio
import pytest
from extensions.rabbitmq import AsyncRabbitMQProducer, AsyncRabbitMQConsumer

@pytest.fixture
def rabbitmq_producer():
    producer = AsyncRabbitMQProducer('amqp://guest:guest@localhost/')
    return producer

@pytest.fixture
def rabbitmq_consumer():
    consumer = AsyncRabbitMQConsumer('amqp://guest:guest@localhost/')
    return consumer

@pytest.mark.asyncio
async def test_rabbitmq_producer_and_consumer(rabbitmq_producer, rabbitmq_consumer):
    # Define a test message
    test_message = "Hello, RabbitMQ!"
    print('test')
    # Send message using the producer
    async with rabbitmq_producer as producer:
        await producer.publish('test_queue', test_message)

    # Define a callback function to receive and verify the message
    received_messages = []
    async def callback(message):
        received_messages.append(message)
        print(message)
        await rabbitmq_consumer.stop()

    # Consume the message using the consumer
    await rabbitmq_consumer.connect()
    await rabbitmq_consumer.consume('test_queue', callback)

    # Allow some time for the message to be received
    await asyncio.sleep(2)

    # Check if the message was received correctly
    assert len(received_messages) == 1
    assert received_messages[0] == test_message

# Run the tests
if __name__ == "__main__":
    pytest.main([__file__])
