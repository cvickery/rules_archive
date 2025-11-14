#! /usr/local/bin/python3
# OBSOLETE Separate schemas for each archive date no longer being use.
""" Generate descriptive statistics for one of the tables in a transfer archive schema.
"""
import psycopg
import sys


# statistics()
# -------------------------------------------------------------------------------------------------
def statistics(schema, table_name):
  """Return mean, median, and frequency distribution of rows per key for a table.
  """
  query = f"""
    WITH rule_key_counts AS (
        -- First, count rows per rule_key
        SELECT
            rule_key,
            COUNT(*) AS row_count
        FROM
            {schema}.{table_name}
        GROUP BY
            rule_key
    ),
    distribution AS (
        -- Then, count rule_keys per row_count
        SELECT
            row_count,
            COUNT(*) AS frequency
        FROM
            rule_key_counts
        GROUP BY
            row_count
        ORDER BY
            row_count
    ),
    stats AS (
        SELECT
            AVG(row_count) AS mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY row_count) AS median
        FROM
            rule_key_counts
    )
    -- Combine the results with an ordering column
    SELECT row_count, frequency, mean, median FROM (
        -- Show the statistics (with sort_order = 1 to appear first)
        SELECT 1 AS sort_order, 'Statistics' AS type, NULL AS row_count, NULL AS frequency,
            (SELECT mean FROM stats) AS mean,
            (SELECT median FROM stats) AS median
        UNION ALL
        -- Show the distribution (with sort_order = 2 to appear after statistics)
        SELECT 2 AS sort_order, 'Distribution' AS type, row_count, frequency,
               NULL AS mean, NULL AS median
        FROM distribution
    ) combined_results
    ORDER BY sort_order, row_count;
    """

  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor() as cursor:
      cursor.execute(query)
      mean, median = cursor.fetchone()[-2:]
      distribution = dict()
      for row in cursor:
        distribution[row[0]] = row[1]
  return mean, median, distribution


# main()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':
  if len(sys.argv) == 2:
    schema = sys.argv[1]
  else:
    # This schema might work
    schema = 'a20250417'

  for table_name in ['source_courses', 'destination_courses']:
    mean, median, distribution = statistics(schema, table_name)
    print(f'{table_name}: {mean:.4} {median:.2}')
    for index, value in distribution.items():
      print(f'[{index}] {value:9,}')
