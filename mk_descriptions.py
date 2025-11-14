#! /usr/local/bin/python3
# OBSOLETE: Use the module of the same name in the rule_descriptions project instead.
"""Generate the canonical description for each rule in a schema’s transfer_rules table.
"""

import psycopg
import shutil

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from psycopg.rows import dict_row


@dataclass
class Context:
    source_courses: dict
    destination_courses: dict
    transfer_rules: dict


# Module-wide db access
conn = psycopg.connect('dbname=cuny_curriculum', autocommit=True)
cursor = conn.cursor(row_factory=dict_row)

# The cuny_courses cache will be used regardless of the schema being processed
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

Course = namedtuple('Course', 'institution course title')


# oxfordize()
# -------------------------------------------------------------------------------------------------
def oxfordize(source_list: list, list_type: str = 'and') -> str:
  """Apply oxford-comma pattern to a list of strings."""
  sentence = ', '.join([' '.join(q) if isinstance(q, tuple) else q for q in source_list])
  if comma_count := sentence.count(','):
    assert list_type.lower() in ['and', 'or'], f'{sentence=} {comma_count=} {list_type=}'
    conjunction_str = f' {list_type}'
    if comma_count == 1:
      return sentence.replace(',', conjunction_str)
    else:
      last_comma = sentence.rindex(',') + 1
      return sentence[:last_comma] + conjunction_str + sentence[last_comma:]
  else:
    return sentence


# grade_restriction()
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


def describe(rule_key: str, ctx: Context) -> str:
  """Gather source and destination course_id:offer_nbr values, and format the rule description.
  """
  ctx.transfer_rules[rule_key] = (f'{oxfordize(ctx.source_courses[rule_key])}'
                                  f' => '
                                  f'{oxfordize(ctx.destination_courses[rule_key])}')
  return ctx.transfer_rules[rule_key]


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
  parser.add_argument('--update_db', '-up', action='store_true')
  parser.add_argument('rule_keys', nargs='*', default=['all'])
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

  print(f'Generate descriptions for {schema_name}')

  # Create the context for this schema
  ctx = Context(defaultdict(list), defaultdict(list), dict())
  # source_courses
  cursor.execute(f'select * from {schema_name}.source_courses')
  s = '' if cursor.rowcount == 1 else 's'
  print(f'{cursor.rowcount:,} source course{s}')
  for source_course in cursor:
    try:
      course_details = courses_cache[(source_course['course_id'], source_course['offer_nbr'])]
      course_details['grade_restriction'] = grade_restriction(source_course['min_grade'],
                                                              source_course['max_grade'])
      status = '' if course_details['is_active'] else '[Inactive]'
    except KeyError:
      course_details = {'is_active': False,
                        'course': 'Unknown',
                        'status': 'Inactive',
                        'grade_restriction': ''}
    ctx.source_courses[source_course['rule_key']].append(f'{course_details['course']}'
                                                         f'{course_details['grade_restriction']}'
                                                         f'{status}')
  # destination_courses
  cursor.execute(f'select * from {schema_name}.destination_courses')
  s = '' if cursor.rowcount == 1 else 's'
  print(f'{cursor.rowcount:,} destination course{s}')
  for destination_course in cursor:
    try:
      course_details = courses_cache[(destination_course['course_id'],
                                      destination_course['offer_nbr'])]
      status = '' if course_details['is_active'] else '[Inactive]'
      status += '[MESG]' if course_details['is_mesg'] else ''
      status += '[BKCR]' if course_details['is_bkcr'] else ''
    except KeyError:
      course_details = {'course': 'Unknown'}
      status = 'Inactive'
    ctx.destination_courses[destination_course['rule_key']].append(f'{course_details['course']}'
                                                                   f'{status}')
  # transfer_rules
  cursor.execute(f'select * from {schema_name}.transfer_rules')
  s = '' if cursor.rowcount == 1 else 's'
  print(f'{cursor.rowcount:,} transfer rule{s}')
  ctx.transfer_rules = {row['rule_key']: row['description'] for row in cursor}

  rule_keys = args.rule_keys
  do_update = False

  if 'all' in rule_keys:
    do_update = args.update_db  # Have to ask for it explicitly
    print(f'Generating all descriptions with {do_update=}')
    terminal_width = shutil.get_terminal_size().columns
    max_desc_width = terminal_width - 24 - 1  # 24 for rule_key, 1 space

    # Generate the descriptions
    for rule_key in ctx.transfer_rules:
      description = describe(rule_key, ctx)
      if not do_update:
        print(f'\x1b[2K\r{rule_key:24} {description[:max_desc_width]}', end='', flush=True)

    if do_update:
      # Bulk update the schema’s transfer_rules table, 100K rows at a time.
      print('\nUpdate db')
      sql = psycopg.sql
      transfer_rules_list = list(ctx.transfer_rules.items())
      chunk_size = 100_000
      num_rules = len(transfer_rules_list)
      for index in range(0, num_rules, chunk_size):
        print(f'\r{index:,}/{num_rules:,}', end='')
        chunk = transfer_rules_list[index:index + chunk_size]
        values_sql = sql.SQL(', ').join(sql.SQL('({},{})').format(sql.Literal(rule_key),
                                                                  sql.Literal(description))
                                        for rule_key, description in chunk)
        query = sql.SQL("""
        update {schema_name}.transfer_rules as t
           set description = v.description
          from (values {values}) as v(rule_key, description)
         where t.rule_key = v.rule_key
        """).format(schema_name=sql.Identifier(schema_name), values=values_sql)
        cursor.execute(query)
    print()
    exit()

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
      if ((src[0:3].lower() == args.sending_institution[0:3].lower()) and
          (dst[0:3].lower() == args.receiving_institution[0:3].lower())):
        if sending:
          rule_keys.append(rule_key)
        if receiving:
          rule_keys.append(rule_key)

  num_rules = len(rule_keys)
  if num_rules:
    s = '' if num_rules == 1 else 's'
    num_keys = len(rule_keys)
    n = 0
    for rule_key in sorted(rule_keys):
      n += 1
      description = describe(rule_key, ctx)
      print(f'\r{n:,}/{num_keys:,} ', end='')
      if do_update:
        cursor.execute(f"""
        update {schema_name}.transfer_rules
           set description = %s
         where rule_key = %s
        """, (description, rule_key))
      else:
        print(f'{rule_key:22} {description:100}', end='')
  else:
    print('No matching rules')
  print()
