# Build transfer rule tables from archived CSV files.

## Project Replaced!

See the [rule_changes](https://github.com/cvickery/rule_changes) for the current project used for showing transfer rule changes at CUNY.

***

When the _cuny\_curriculum_ database is updated, three tables (source\_courses, transfer\_rules, and destination\_courses) are extracted to CSV files, compressed, and saved in a `rules_archive` directory. This project creates a set of three tables from a set of archive files. Each set of tables is saved in its own schema, named for the date the archive set was created.

In the public schema, the transfer\_rules table contains a good deal of redundant information, and only the rule\_key and effective\_date fields are archived/ The archived source\_courses/destination\_courses reference rules by their natural key (`rule_key`) values rather than by the surrogate key (`id`) field used in the public schema.
