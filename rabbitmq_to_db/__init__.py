import asyncio
import contextlib
import json
from datetime import datetime

import psycopg2
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage

from .handlers import Handler, PostgresHandler

####################################################################################################

class PostgresConnector:
  def __init__(self, *args, **kwargs) -> None:
    self.args = args
    self.kwargs = kwargs
    self.connect()

  def connect(self):
    self.pg = psycopg2.connect(*self.args, **self.kwargs)

  def connection(self):
    return self.pg

####################################################################################################

class RabbitMqToDb:
  def __init__(self, postgres_connector: PostgresConnector, flush_count: int = 100, flush_time: float = 5.0) -> None:
    self.postgres_connector = postgres_connector
    self.flush_count = flush_count
    self.flush_time = flush_time

    self.lock = asyncio.Lock()
    self.counter_condition = asyncio.Condition()
    self.latest_message: AbstractIncomingMessage = None
    self.message_counter = 0

    self.handlers: dict[str, Handler] = {}

  def add_handler(self, key: str, handler: Handler):
    assert isinstance(handler, PostgresHandler)
    self.handlers[key] = handler

  def add_postgres_handler(self, table: str, *, key: str = None, **kwargs):
    if key is None:
      key = table
    self.add_handler(key, PostgresHandler(table=table, **kwargs))

  async def consume(self, channel: AbstractChannel, queue_name: str):
    await channel.set_qos(prefetch_count=self.flush_count)
    # TODO: Support creating queue and bindings?
    queue = await channel.get_queue(queue_name)
    await queue.consume(self._on_message)

  async def _on_message(self, message: AbstractIncomingMessage) -> None:
    (key, format) = message.routing_key.split('.')[-2:]
    match format:
      case 'json':
        data = json.loads(message.body)

    # TODO: Abort if the handler was not found.
    handler = self.handlers[key]

    if handler.needs_time:
      data['time'] = datetime.fromtimestamp(data.get('time', message.headers['timestamp_in_ms'] / 1000))

    async with self.lock:
      handler.add(data)
      self.latest_message = message
      async with self.counter_condition:
        self.message_counter += 1
        self.counter_condition.notify_all()

  async def wait_for_count(self, count):
    async with self.counter_condition:
      while self.message_counter < count:
        await self.counter_condition.wait()

  async def flush_loop(self) -> None:
    while True:
      with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(self.wait_for_count(self.flush_count), self.flush_time)
      await self.flush()

  async def flush(self):
    async with self.lock:
      if self.latest_message is None:
        return

      await asyncio.to_thread(self._db_work)
      await self.latest_message.ack(multiple=True)

      self.latest_message = None
      async with self.counter_condition:
        self.message_counter = 0
        self.counter_condition.notify_all()

  def _db_work(self, one_by_one=False):
    try:
      # TODO: Support different types of handlers.
      pg = self.postgres_connector.connection()
      cur = pg.cursor()
      for handler in self.handlers.values():
        handler.flush(cur, one_by_one)
      pg.commit()
    except psycopg2.DatabaseError as error:
      pg.rollback()
      if 'ON CONFLICT DO UPDATE command cannot affect row a second time' in error.pgerror and not one_by_one:
        self._db_work(True)
      else:
        raise error
    finally:
      if cur:
        cur.close()
