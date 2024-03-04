from abc import abstractmethod

import psycopg
from psycopg import sql

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
      copy_table = self.temp_table
      self.create_table_sql = sql.SQL("CREATE TEMP TABLE IF NOT EXISTS {temp_table} (LIKE {table} INCLUDING DEFAULTS)").format(
          table=sql.Identifier(self.table),
          temp_table=sql.Identifier(self.temp_table))
      self.insert_sql = sql.SQL("INSERT INTO {table} SELECT * FROM {temp_table} ON CONFLICT ({conflict_fields}) DO UPDATE SET {updates}").format(
          table=sql.Identifier(self.table),
          temp_table=sql.Identifier(self.temp_table),
          conflict_fields=sql.SQL(', ').join(sql.Identifier(f) for f in key_fields),
          updates=sql.SQL(', ').join(sql.SQL('{field}=EXCLUDED.{field}').format(field=sql.Identifier(f)) for f in value_fields))
      self.truncate_sql = sql.SQL('TRUNCATE {}').format(sql.Identifier(self.temp_table))
    else:
      copy_table = self.table

    self.copy_sql = sql.SQL("COPY {table} ({fields}) FROM STDIN").format(
        table=sql.Identifier(copy_table),
        fields=sql.SQL(', ').join(sql.Identifier(f) for f in self.fields))

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
      await cursor.execute(self.create_table_sql)

    async with cursor.copy(self.copy_sql) as copy:
      for row in values:
        await copy.write_row(row)

    if self.temp_table:
      await cursor.execute(self.insert_sql)
      await cursor.execute(self.truncate_sql)
