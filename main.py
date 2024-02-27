#!/usr/bin/env python3
import asyncio
import os

import aio_pika
from dotenv import load_dotenv

from rabbitmq_to_db import PostgresConnector, RabbitMqToDb

####################################################################################################

async def main() -> None:
  load_dotenv()
  POSTGRES_DSN = os.getenv('POSTGRES_DSN')
  RABBITMQ_DSN = os.getenv('RABBITMQ_DSN')
  QUEUE = os.getenv('QUEUE')
  FLUSH_COUNT = 100
  FLUSH_TIME = 5

  rmq2db = RabbitMqToDb(PostgresConnector(POSTGRES_DSN), flush_count=FLUSH_COUNT, flush_time=FLUSH_TIME)
  rmq2db.add_postgres_handler('roomtemp', key_fields=['time', 'room_id'], value_fields=['temp'])
  rmq2db.add_postgres_handler('strom', key_fields=['time'], value_fields=['stand', 'leistung'])

  connection = await aio_pika.connect(RABBITMQ_DSN)
  async with connection:
    channel = await connection.channel()
    await rmq2db.consume(channel, QUEUE)
    asyncio.create_task(rmq2db.flush_loop(), name='Flusher')
    print(" [*] Waiting for messages. To exit press CTRL+C")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
