#! /usr/local/bin/python3
"""Generate the canonical description for each rule in a schema’s transfer_rules table.
"""

import psycopg

from argparse import ArgumentParser
from psycopg.rows import dict_row

# Cursor for accessing the cuny_courses table in the public schema
conn = psycopg.connect('dbname=cuny_curriculum')
cursor = conn.cursor(row_factory=dict_row)

cursor.execute("""
select course_id, offer_nbr, institution, discipline||' '||catalog_number as course, title,
       course_status = 'A' as is_active,
       designation in ('MLA', 'MNL') as is_mesg,
       attributes ~* 'bkcr' as is_bkcr
  from cuny_courses""")
courses_cache = {(row['course_id'], row['offer_nbr']): row for row in cursor}

# Some basic characteristics of the CUNY catalog
# print(f'{len(courses_cache):8,} courses')
# print(f'{sum(1 for c in courses_cache if c['is_active']):8,} active')
# print(f'{sum(1 for c in courses_cache if c['is_active'] and c['is_mesg']):8,} message')
# exit(f'{sum(1 for c in courses_cache if c['is_active'] and c['is_bkcr']):8,} blanket')


# _grade_restriction()
# -------------------------------------------------------------------------------------------------
def grade_restriction(min_grade: float, max_grade: float) -> str:
  """ Convert numerical gpa range to description of required grade in letter-grade form.
      Returned string is empty for no restriction. Otherwise it’s a square-bracketed phrase
      formatted to follow the course string, such as “CSCI 100 [B or above]”
  """
  # Convert GPA values to letter grades by table lookup.
  # int(round(3×GPA)) gives the index into the letters table.
  """
          GPA  3×GPA  Index  Letter
          4.3   12.9     13      A+
          4.0   12.0     12      A
          3.7   11.1     11      A-
          3.3    9.9     10      B+
          3.0    9.0      9      B
          2.7    8.1      8      B-
          2.3    6.9      7      C+
          2.0    6.0      6      C
          1.7    5.1      5      C-
          1.3    3.9      4      D+
          1.0    3.0      3      D
          0.7    2.1      2      D-
  """
  grade_map = {
    0.7: "D-",
    1.0: "D",
    1.3: "D+",
    1.7: "C-",
    2.0: "C",
    2.3: "C+",
    2.7: "B-",
    3.0: "B",
    3.3: "B+",
    3.7: "A-",
    4.0: "A"
  }

  def is_close(a, b, eps=1e-6):
    return abs(a - b) < eps

  def label(grade: float):
    for k, v in grade_map.items():
      if grade <= k:
        return v
    raise ValueError(f'{grade:.2} is not a valid grade value')

  min_grade = float(min_grade)
  max_grade = float(max_grade)
  assert min_grade <= max_grade, f'Invalid min/max grade pair: ({min_grade}, {max_grade})'
  if is_close(min_grade, 0.0):
    if is_close(max_grade, 4.0):
      return ''  # Common case: no grade restriction
    else:
      return f' [below {label(max_grade)}]'
  elif is_close(max_grade, 4.0):
    return f' [{label(min_grade)} or above]'
  elif label(min_grade) == label(max_grade):
    return f' [exactly {label(min_grade)}]'  # Unlikely
  else:
    # Useful for detecting overlapping ranges across multiple rules
    return f' [between {label(min_grade)} and {label(max_grade)}]'


def describe(schema_name: str, rule_key: str) -> str:
  """Gather source and destination course_id:offer_nbr values, and format the rule description.
  """
  cursor.execute(f"""
  select *
    from  {schema_name}.source_courses
    where rule_key = %s
  """, (rule_key, ))
  source_courses = cursor.fetchall()
  for source_course in source_courses:
    course_details = courses_cache[(source_course['course_id'], source_course['offer_nbr'])]
    for detail in ['course', 'title', 'is_active', 'is_mesg', 'is_bkcr']:
      source_course[detail] = course_details[detail]
      source_course['grade_restriction'] = grade_restriction(source_course['min_grade'],
                                                             source_course['max_grade'])
    print(source_course)

  cursor.execute(f"""
  select *
    from  {schema_name}.destination_courses
    where rule_key = %s
  """, (rule_key, ))
  destination_courses = cursor.fetchall()
  for destination_course in destination_courses:
    course_details = courses_cache[(destination_course['course_id'],
                                    destination_course['offer_nbr'])]
    for detail in ['course', 'title', 'is_active', 'is_mesg', 'is_bkcr']:
      destination_course[detail] = course_details[detail]
    print(destination_course)

  return f'{rule_key} is a very nice rule'


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
  schemata = [row['schema_name'] for row in cursor]
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
    rule_keys = [row['rule_key'] for row in cursor]

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
    courses = {(row['course_id'], row['offer_nbr']):
               Course._make([row['institution'][0:3].lower(),
                             row['course'], row['title']]) for row in cursor}

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
      rule_key = row['rule_key']
      src, dst, *_ = rule_key.split(':')
      if ((src[0:3].lower() == args.sending_institution[0:3].lower()) or
          (dst[0:3].lower() == args.receiving_institution[0:3].lower())):
        if sending:
          rule_keys.append(rule_key)
        if receiving:
          rule_keys.append(rule_key)
  num_rules = len(rule_keys)
  s = '' if num_rules == 1 else 's'
  print(f'Describing {len(rule_keys)} rule{s}')
  for rule_key in sorted(rule_keys):
    print(describe(schema_name, rule_key))
