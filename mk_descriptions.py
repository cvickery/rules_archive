#! /usr/local/bin/python3
"""Generate the canonical description for each rule in a schema’s transfer_rules table.
"""

from collections import namedtuple
import psycopg
from argparse import ArgumentParser

# Cursor for accessing the cuny_courses table in the public schema
conn = psycopg.connect('dbname=cuny_curriculum')
cursor = conn.cursor()

Course = namedtuple('Course', 'institution course title')


def mk_description(schema_name: str, rule_key: str) -> str:
  """Gather source and destination course_id:offer_nbr values, and format the rule description.
  """
  cursor.execute(f"""{schema_name}
  """)


# main()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':
  """Given a rule_key or a set of courses, generate the description(s) for applicable rules."""

  # Command line options
  parser = ArgumentParser('Generate rule description')
  parser.add_argument('--schema_name', '-sn')
  parser.add_argument('--sending_institution', '-si', default='QCC01')
  parser.add_argument('--receiving_institution', '-ri', default='QNS01')
  parser.add_argument('--subject', '-su', default='SEYS')
  parser.add_argument('--catalog_number', '-cn', default='^49.*')
  parser.add_argument('--direction', '-di', default='both')
  parser.add_argument('rule_keys', nargs='*')
  args = parser.parse_args()

  # Which schema?
  cursor.execute("""
  select schema_name
    from information_schema.schemata
   where schema_name ~* '^a20'
   order by schema_name
  """)
  schemata = [row[0] for row in cursor]
  if len(schemata) < 1:
    exit('No schemata available')
  if args.schema_name:
    schema_name = args.schema_name
    if schema_name not in schemata:
      exit(f'{schema_name} not found')
  else:
    # Use the most-recent schema available.
    schema_name = schemata[-1]

  rule_keys = args.rule_keys
  if 'all' in rule_keys:
    cursor.execute(f'select rule_key from {schema_name}.transfer_rules order by rule_key')
  rule_keys = [row[0] for row in cursor]
  courses = dict()  # to short-circuit rule_keys lookup below

  if not rule_keys:
    # Allow regex for subject/catalog_nbr
    subject = f'^{args.subject.strip('^$')}$'
    catalog_number = f'^{args.catalog_number.strip('^$')}'
    cursor.execute("""
    select course_id, offer_nbr, discipline||' '||catalog_number as course, title, institution
      from cuny_courses
     where (institution ~* %s or institution ~* %s)
       and discipline ~* %s
       and catalog_number ~* %s
    """, (args.sending_institution, args.receiving_institution, subject, catalog_number))
    match cursor.rowcount:
      case 0:
        exit(f'No matching courses for {args.subject} {args.catalog_number} in'
             f'{args.sending_institution} or {args.receiving_institution}')
      case 1:
        s = ''
      case _:
        s = 's'
    print(f'There are {cursor.rowcount} {args.subject.strip('^$')} '
          f'{args.catalog_number.strip('^$')} course{s}')
    courses = {row[0:2]: Course._make([row[4][0:3].lower(), row[2], row[3]]) for row in cursor}

    for course in courses.values():
      print(f'  {course.institution} {course.course}: {course.title}')

  direction = args.direction.lower()
  sending = 'sending'.startswith(direction) or 'both'.startswith(direction)
  receiving = 'receiving'.startswith(direction) or 'both'.startswith(direction)
  if not (sending or receiving):
    exit(f'“{args.direction}” is not “sending”, “receiving”, or “both”')

  for course in courses:
    cursor.execute(f"""
    select distinct rule_key from {schema_name}.source_courses
     where course_id = %s and offer_nbr = %s
    union
    select distinct rule_key from {schema_name}.destination_courses
     where course_id = %s and offer_nbr = %s
    """, course + course)
    for row in cursor:
      rule_key = row[0]
      src, dst, *_ = rule_key.split(':')
      if ((src[0:3].lower() == args.sending_institution[0:3].lower()) or
          (dst[0:3].lower() == args.receiving_institution[0:3].lower())):
        if sending:
          rule_keys.append(rule_key)
        if receiving:
          rule_keys.append(rule_key)
  num_rules = len(rule_keys)
  s = '' if num_rules == 1 else 's'
  print(f'{len(rule_keys)} rule{s} from {args.sending_institution[0:3].lower()} to '
        f'{args.receiving_institution[0:3].lower()}')
  for rule_key in sorted(rule_keys):
    print(rule_key)
