#! /usr/local/bin/python3
# OBSOLETE Separate schemas for each archive date no longer being used.

"""Drop all schemata created by mk_tables."""

import psycopg

if __name__ == '__main__':
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor() as cursor:
      cursor.execute("""
      select schema_name
        from information_schema.schemata
       where schema_name ~* '^a20' -- names are “aYYYY-MM-DD”, so this selects all in 21st century
       """)
      rows = cursor.fetchall()
      for row in rows:
        schema_name = row[0]
        print(f'Drop {schema_name}')
        cursor.execute(f'drop schema {schema_name} cascade')
