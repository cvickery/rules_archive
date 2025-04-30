#! /usr/local/bin/python3
"""Give a date, find the closest rules_archive set, and build a date-stamped
   version of the transfer_rules, source_courses, and destination_courses tables.

   Each archive gets its own date-named schema.
"""

import bz2
import csv
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

  # Get requested archive date
  parser = ArgumentParser('Create a set of transfer rule tables ')

  parser.add_argument('--archive_date', '-ad')
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
  print(f'{archive_date=}')

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
  schema = f'a{archive_date.replace('-', '')}'
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor() as cursor:
      cursor.execute(f"drop schema if exists {schema} cascade")
      cursor.execute(f"create schema {schema}")

      # transfer_rules
      cursor.execute(f"""
      create table {schema}.transfer_rules (
        rule_key                text primary key,
        effective_date          date
      )
      """)
      with bz2.open(effective_dates_archive, mode='rt') as infile:
        print('transfer_rules:      ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          cursor.execute(f"""
          insert into {schema}.transfer_rules values (%s, %s)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema}.transfer_rules
      """)
      print(f'{cursor.fetchone()[0]:,}')

      # source_courses
      cursor.execute(f"""
      create table {schema}.source_courses (
        id          serial primary key,
        rule_key    text references {schema}.transfer_rules,
        course_id   integer,
        offer_nbr   integer,
        min_credits real,
        max_credits real,
        credit_src  text,
        min_gpa     real,
        max_gpa     real
      )
      """)
      with bz2.open(source_archive, mode='rt') as infile:
        print('source courses:      ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          cursor.execute(f"""
          insert into {schema}.source_courses values (default, %s, %s, %s, %s, %s, %s, %s, %s)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema}.source_courses
      """)
      print(f'{cursor.fetchone()[0]:,}')

      # destination_courses
      cursor.execute(f"""
      create table {schema}.destination_courses (
        id        serial primary key,
        rule_key  text references {schema}.transfer_rules,
        course_id integer,
        offer_nbr integer,
        credits   real
      )
      """)
      with bz2.open(destination_archive, mode='rt') as infile:
        print('destination courses: ', end='')
        sys.stdout.flush()
        reader = csv.reader(infile)
        for line in reader:
          cursor.execute(f"""
          insert into {schema}.destination_courses values(default, %s, %s, %s, %s)
          """, line)
      cursor.execute(f"""
      select count(*) from {schema}.destination_courses
      """)
      print(f'{cursor.fetchone()[0]:,}')

  # Show mean, median, and frequency distribution for number of source|destination courses per rule?
  if args.statistics:
    for table_name in ['source_courses', 'destination_courses']:
      mean, median, distribution = statistics(schema, table_name)
      print(f'{table_name}: {mean:.4} {median:.2}')
      for index, value in distribution.items():
        print(f'[{index}] {value:9,}')
