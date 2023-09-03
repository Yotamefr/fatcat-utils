from fatcat_utils.rabbitmq import RabbitMQHandler
import pytest


@pytest.mark.asyncio
async def test_publisher():
    rabbit = RabbitMQHandler()
    await rabbit.connect()
    await rabbit.send_to_queue({
        "stringtest": "123",
        "integertest": "5",
        "FATCAT_QUEUE_TAG": "idrk"
    }, "incoming")
    await rabbit.close()
