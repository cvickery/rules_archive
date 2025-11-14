#! /usr/local/bin/python3
# OBSOLETE: The changed_rules module does all this more efficiently and better.
"""Given a date, find the closest rules_archive set, and build a date-stamped
   version of the transfer_rules, source_courses, and destination_courses tables.

   Each archive gets its own date-named schema.
"""

import bz2
import csv
import datetime
import psycopg
import subprocess
import sys

from argparse import ArgumentParser
from bisect import bisect_left
from pathlib import Path
from statistics import statistics

# main()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':

  # Get list of available archives
  archive_dir = Path(Path.home(), 'Projects/cuny_curriculum/rules_archive')
  if not archive_dir.is_dir():
    exit('Rules archive dir not found')
  archive_dates = []
  for file_path in archive_dir.glob('*effective*'):
    archive_dates.append(file_path.name[0:10])
  archive_dates.sort()
  print(f'{len(archive_dates)} archives between {archive_dates[0]} to {archive_dates[-1]}')

  # Get requested archive date: default is the latest one available.
  parser = ArgumentParser('Create a set of transfer rule tables ')

  parser.add_argument('--archive_date', '-ad', default=f'{datetime.date.today()}')
  parser.add_argument('--statistics', '-s')
  args = parser.parse_args()
  try:
    result = subprocess.run(['date', '--date', args.archive_date, '+%Y-%m-%d'],
                            capture_output=True,
                            text=True,
                            check=True)
  except subprocess.CalledProcessError:
    exit(f'Invalid archive date string: {args.archive_date}')
  archive_target = result.stdout.strip()

  # Find the last archive at or before archive_target
  archive_date_index = min(bisect_left(archive_dates, archive_target), len(archive_dates) - 1)
  archive_date = archive_dates[archive_date_index]
  schema_name = f'a{archive_date.replace('-', '')}'
  print(f'{archive_date=} {schema_name}')

  # Be sure all three archive files are available
  fail = False
  source_archive = Path(archive_dir, f'{archive_date}_source_courses.csv.bz2')
  if not source_archive.is_file():
    print(f'{archive_date}_source_courses.csv.bz2 is not a file')
    fail = True

  destination_archive = Path(archive_dir, f'{archive_date}_destination_courses.csv.bz2')
  if not destination_archive.is_file():
    print(f'{archive_date}_destination_courses.csv.bz2 is not a file')
    fail = True

  effective_dates_archive = Path(archive_dir, f'{archive_date}_effective_dates.csv.bz2')
  if not source_archive.is_file():
    print(f'{archive_date}_effective_dates.csv.bz2 is not a file')
    fail = True

  if fail:
    exit()

  # Create the schema and build the tables
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor() as cursor:
      cursor.execute(f"drop schema if exists {schema_name} cascade")
      cursor.execute(f"create schema {schema_name}")

      # transfer_rules
      cursor.execute(f"""
      create table {schema_name}.transfer_rules (
        id                      serial primary key,
        rule_key                text unique,
        effective_date          date,
        description             text default ''
      )
      """)
      with bz2.open(effective_dates_archive, mode='rt') as infile:
        print('transfer_rules:      ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          cursor.execute(f"""
          insert into {schema_name}.transfer_rules values (default, %s, %s, default)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema_name}.transfer_rules
      """)
      print(f'{cursor.fetchone()[0]:,}')

      # source_courses
      cursor.execute(f"""
      create table {schema_name}.source_courses (
        id          serial primary key,
        rule_key    text references {schema_name}.transfer_rules(rule_key),
        src_inst    text,
        dst_inst    text,
        course_id   integer,
        offer_nbr   integer,
        min_credits real,
        max_credits real,
        credit_src  text,
        min_grade   real,
        max_grade   real      )
      """)
      with bz2.open(source_archive, mode='rt') as infile:
        print('source courses:      ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          # Break out fields for the source and destination institutions
          rule_key = line[0]
          src_inst = rule_key[0:5]
          dst_inst = rule_key[6:11]
          line = [line[0], src_inst, dst_inst] + line[1:]
          # Normalize grade range
          line[-2] = 0. if float(line[-2]) < 0.7 else float(line[-2])
          line[-1] = min(float(line[-1]), 4.0)
          cursor.execute(f"""
          insert into {schema_name}.source_courses
                 values (default, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema_name}.source_courses
      """)
      print(f'{cursor.fetchone()[0]:,}')

      # destination_courses
      cursor.execute(f"""
      create table {schema_name}.destination_courses (
        id        serial primary key,
        rule_key  text references {schema_name}.transfer_rules(rule_key),
        course_id integer,
        offer_nbr integer,
        credits   real      )
      """)
      with bz2.open(destination_archive, mode='rt') as infile:
        print('destination courses: ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          cursor.execute(f"""
          insert into {schema_name}.destination_courses values(default, %s, %s, %s, %s)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema_name}.destination_courses
      """)
      print(f'{cursor.fetchone()[0]:,}')

  # Show mean, median, and frequency distribution for number of source|destination courses per rule?
  if args.statistics:
    for table_name in ['source_courses', 'destination_courses']:
      mean, median, distribution = statistics(schema_name, table_name)
      print(f'{table_name}: {mean:.4} {median:.2}')
      for index, value in distribution.items():
        print(f'[{index}] {value:9,}')
