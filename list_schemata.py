#! /usr/local/bin/python3

import psycopg

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor() as cursor:
    cursor.execute("""
    select schema_name
      from information_schema.schemata
     where schema_name ~* '^a20'
    order by schema_name""")
    for row in cursor:
      print(row[0])
