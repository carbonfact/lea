# School example

Let's back to school with an example and demonstrate the use of tests.

## Bootstrapping

First, usual bootstrapping of database for `lea`, it goes by creating a `.env`:

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=school.db
" > .env
```

This example uses DuckDB as the datawarehouse.

You can run the scripts:

```sh
lea run
```

Lea will create schema in DuckDB, create audit tables based on scripts definition.
Once audit tables are generated, lea run tests against audit tables.

Let's review some tests together.

## Exploration

Vizualize students in this school :

```sh
python -c import duckdb; print(duckdb.connect('school_emilien.db').execute('SELECT student_id, first_name, last_name, university FROM staging.students').df())
```

```
   student_id first_name last_name                             university
0           1     Lauren    Levine                    Stanford University
1           2     Daniel     Lopez  Massachusetts Institute of Technology
2           3    Melanie    Foster      University of California Berkeley
3           4    Gabriel     Cooke                     Harvard University
4           5       Anne    Porter                     Harvard University
5           6        Amy       Lee                   Princeton University
6           7    Rebecca    Chavez                   Princeton University
```

You can see some students, let's review their grades !

```sh
python -c "import duckdb; print(duckdb.connect('school_emilien.db').execute('SELECT student_id, student_name, class_name, semester, average_grade FROM core.yearly_results USING SAMPLE 5').df())"
```

```
   student_id   student_name   class_name    semester  average_grade
0           6        Amy Lee  Mathematics  Semester 1           59.0
1           5    Anne Porter   Literature  Semester 2          100.0
2           5    Anne Porter      Physics  Semester 2           46.0
3           1  Lauren Levine      Biology  Semester 1           28.5
4           1  Lauren Levine   Literature  Semester 2           52.5
```

## Tests

Awesome ! Pretty good students, let's review some tests made.

```sql
WITH raw_students AS (
    SELECT * FROM './seeds/raw_students.csv'
)

SELECT
    -- #UNIQUE
    -- #NO_NULLS
    id AS student_id,
    first_name,
    -- #UNIQUE_BY(first_name)
    last_name,
    -- #SET{'Stanford University', 'University of California Berkeley', 'Princeton University', 'Harvard University', 'Massachusetts Institute of Technology'}
    university,
FROM raw_students;
```

During the WAP pattern, thoses checks will ensure Data Quality making assertions tests.

Here for instance, the staging model during Audit step will ensure that :

- `student_id` values are not null and unique
- `last_name` are unique by first_name
- `university` values are in the exposed list

## WAP and break and audit

Let's break a test on purpose for demonstratation :

Under `seeds/raw_students`, let's add a new student university :

```sh
echo "8,Andy,Bernard,Cornell University,23" >> seeds/raw_students.csv
```

Let's run again scripts :

```sh
lea run
```

Cornell University is not allowed here :

```
           ✋ Early ending because an error occurred
           😴 Ending session
           STOPPED school_emilien.core.yearly_results___audit
           SUCCESS school_emilien.core.yearly_results___audit, contains 112 rows
           ERRORED school_emilien.tests.staging__students__university___set___audit
                      university
           0  Cornell University
           ❌ Finished, took less than a second 🚀

```

Remove last line added to restore source:

```sh
sed -i '' '$d' seeds/raw_students.csv
```

As audit prevented from corrupting intermediate tables, your tables
are still healthy.

## Restart to get a fresh environment

However, as our audit tables are messy and not sync with source, let's rerun them:

```sh
lea run --restart
```

It will flush the audit table, as if it was a fresh start.

## Skipping feature

You might think now, "hey, how does lea know which audit table should be run again
or not ?". That's an excellent question !

If the script have been modified earlier than materialization date
of the table, it will skip the auditing, as nothing changed from the logic !

Let's see together with a closer look.

You can vizualize the scholarships award winners :

Have you noticed that `lea` automatically skip table that are not relevant to process
dag run ?

You can see by practicing. First vizualize award winner of scholarships :

```sh
 python -c "import duckdb; print(duckdb.connect('school_emilien.db').execute('SELECT student_name, domain, scholarship_amount FROM analytics.scholarship_award WHERE domain = \'Economics\'').df())"
```

Each top performing student get a 1000$ grant and 2nd gets a 500$ grant.

Review the amount of money spent :

```sh
lea run --select analytics.finance.expenses
python -c "import duckdb; print(duckdb.connect('school_emilien.db').execute('SELECT total_expenses FROM analytics.finance__expenses').df())"
```

```
   total_expenses
0         12000.0

```

Let's modify a script and demonstrate that lea will run again only scripts that have been modified.

Good news, the academy got 2x more budget this year ! You can deliver a scholarship award
for top performing student **each semester**.

To apply this evolution, uncomment all lines under `analytics.scholarship_award` with `--uncomment here` and comment the `--comment here` ones

```sh
sed -i '' '/--comment here/s/^/--/' scripts/analytics/scholarship_award.sql
sed -i '' '/--uncomment here/s/-- //' scripts/analytics/scholarship_award.sql
```

Then run on
