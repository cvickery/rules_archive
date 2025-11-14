#! /usr/local/bin/bash
# OBSOLETE Separate schemas for each archive date no longer being used.

# Usage: ./log_bogus_rules.sh [schema]
schema_name="$1"
if [[ -z "$schema_name" ]]; then
  echo "Usage: $0 schema_name" >&2
  exit 1
fi

dbname="cuny_curriculum"
report_dir="$(pwd)/reports"

psql "$dbname" -c "
copy (
  SELECT DISTINCT rule_key, sc.course_id, sc.offer_nbr
  FROM ${schema_name}.source_courses sc
  LEFT JOIN public.cuny_courses cc
    ON sc.course_id = cc.course_id AND sc.offer_nbr = cc.offer_nbr
  WHERE cc.course_id IS NULL
  )
  to '${report_dir}/${schema_name}.source_courses.csv' csv
"

psql "$dbname" -c "
copy (
  SELECT DISTINCT rule_key, dc.course_id, dc.offer_nbr
  FROM ${schema_name}.destination_courses dc
  LEFT JOIN public.cuny_courses cc
    ON dc.course_id = cc.course_id AND dc.offer_nbr = cc.offer_nbr
  WHERE cc.course_id IS NULL
  )
  to '${report_dir}/${schema_name}.destination_courses.csv' csv
"
wc -l "${report_dir}/${schema_name}"*
