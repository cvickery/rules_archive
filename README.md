# Build transfer rule tables from archived CSV files.

When the cuny_curriculum database is updated, three tables (source_courses, transfer_rules, and destination_courses) are extracted to CSV files, compressed, and saved in a `rules_archive` directory. This project creates a set of three tables from a set of archive files. Each set of tables is saved in its own schema, named for the date the archive set was created.

In the public schema, the transfer_rules table contains a good deal of redundant information, and only the rule_key and effective_date fields are archived/ The archived source_courses/destination_courses reference rules by their natural key (`rule_key`) values rather than by the surrogate key (`id`) field used in the public schema.
