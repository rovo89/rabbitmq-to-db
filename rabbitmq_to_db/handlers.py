from abc import abstractmethod

import psycopg2.extras

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
  def __init__(self, table: str, key_fields: list[str], value_fields: list[str], template=None, page_size=100) -> None:
    super().__init__()
    self.fields = [*key_fields, *value_fields]
    # TODO: Escape table and field names?
    self.sql = f"INSERT INTO {table} ({', '.join((self.fields))}) VALUES %s ON CONFLICT ({', '.join(key_fields)}) DO UPDATE SET {', '.join(map(lambda f: f'{f}=EXCLUDED.{f}', value_fields))}"
    self.template = template
    self.page_size = page_size

  def add(self, data: dict) -> None:
    # TODO: Allow preprocessing via an argument?
    super().add([*map(lambda f: data[f], self.fields)])

  @property
  def needs_time(self) -> bool:
    return 'time' in self.fields

  def flush(self, cursor, one_by_one):
    page_size = 1 if one_by_one else self.page_size
    psycopg2.extras.execute_values(cursor, self.sql, self.values, self.template, page_size)
    # TODO: Should we await that the ACK was successful?
    self.clear()
