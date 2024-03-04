from abc import abstractmethod

import psycopg

####################################################################################################

class Handler:
  def __init__(self) -> None:
    self.clear()

  def add(self, data: any) -> None:
    self.values.append(data)

  def clear(self) -> None:
    self.values = []

  @property
  def needs_time(self) -> bool:
    return False

  @abstractmethod
  def flush(self, **kwargs) -> None: ...

####################################################################################################

class PostgresHandler(Handler):
  def __init__(self, table: str, key_fields: list[str], value_fields: list[str], use_temp_table=True) -> None:
    super().__init__()
    self.table = table
    self.fields = [*key_fields, *value_fields]
    if use_temp_table:
      self.temp_table = 'temp_' + self.table
      self.copy_sql = f"COPY {self.temp_table} ({', '.join((self.fields))}) FROM STDIN"
      self.insert_sql = f"INSERT INTO {self.table} SELECT * FROM {self.temp_table} ON CONFLICT ({', '.join(key_fields)}) DO UPDATE SET {', '.join(map(lambda f: f'{f}=EXCLUDED.{f}', value_fields))}"
      self.truncate_sql = f"TRUNCATE {self.temp_table}"
    else:
      self.copy_sql = f"COPY {self.table} ({', '.join((self.fields))}) FROM STDIN"

  def add(self, data: dict) -> None:
    # TODO: Allow preprocessing via an argument?
    super().add([*map(lambda f: data[f], self.fields)])

  @property
  def needs_time(self) -> bool:
    return 'time' in self.fields

  async def flush(self, cursor: psycopg.AsyncClientCursor, one_by_one: bool):
    if one_by_one:
      for row in self.values:
        await self._insert(cursor, [row])
    else:
      await self._insert(cursor, self.values)

  async def _insert(self, cursor: psycopg.AsyncClientCursor, values: list):
    if self.temp_table:
      await cursor.execute(f'CREATE TEMP TABLE IF NOT EXISTS {self.temp_table} (LIKE {self.table} INCLUDING DEFAULTS)')

    async with cursor.copy(self.copy_sql) as copy:
      for row in values:
        await copy.write_row(row)

    if self.temp_table:
      await cursor.execute(self.insert_sql)
      await cursor.execute(self.truncate_sql)
