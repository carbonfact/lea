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
python -c import duckdb; print(duckdb.connect('school_max.db').execute('SELECT student_id, first_name, last_name, university FROM staging.students').df())
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
python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT student_id, student_name, class_name, semester, average_grade FROM core.yearly_results USING SAMPLE 5').df())"
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

During the Write-Audit-Publish pattern, thoses checks will ensure Data Quality making assertions tests.

Here for instance, the staging model during Audit step will ensure that :

- `student_id` values are not null and unique
- `last_name` are unique by first_name
- `university` values are in the exposed list

## WAP pattern in action - break during auditing

Let's break a test on purpose for demonstration :

Under `seeds/raw_students`, let's add a new student :

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
           STOPPED school_max.core.yearly_results___audit
           SUCCESS school_max.core.yearly_results___audit, contains 112 rows
           ERRORED school_max.tests.staging__students__university___set___audit
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

## Restart Feature demo - Get a fresh environment

However, as our audit tables are messy and not sync with source, let's rerun them:

```sh
lea run --restart
```

It will flush the audit table, as if it was a fresh start.

## Skipping feature demonstration

You might think now, "hey, how does lea know which audit table should be run again
or not ?". That's an excellent question !

Have you noticed that `lea` automatically skip table that are not relevant to process during audits ?

Let's see together with a closer look by practicing with an example !

First you can vizualize award winner of scholarships:

Each top performing student get a 1000$ grant and 2nd gets a 500$ grant.

You can see the winners of the year in `Economics`:

```sh
python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT student_name, domain, scholarship_amount FROM analytics.scholarship_award WHERE domain = \'Economics\'').df())"
```

```
    student_name     domain  scholarship_amount
0   Daniel Lopez  Economics                1000
1  Gabriel Cooke  Economics                 500

```

You can review the total amount of money spent :

```sh
lea run --select analytics.finance.expenses
python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT total_expenses FROM analytics.finance__expenses').df())"
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

Then run again the finance script.

```sh
lea run --select  analytics.finance.expenses
```

Oh no, the budget test is failing ! Modify the value under `scripts/tests/budget.sql` :

```sh
sed -i '' '/--comment here/s/^/--/' scripts/tests/budget.sql
sed -i '' '/--uncomment here/s/-- //' scripts/tests/budget.sql
```

Now let's run again the scripts :

```sh
lea run
```

Everything pass 🎉

Look closely : **audit tables haven't been materialized again for `school*max.core.yearly_results***audit`
as they were already existing and the script modification date was **anterior** to materialization date !

But it would has been executed, if the script was modified **prior** last table materialization.

You can check the table materialization date with :

```
python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT MAX(_materialized_timestamp) AS last_materialized FROM analytics.scholarship_award').df())"
```

        last_materialized

0 2025-03-14 00:31:28.114

Now the school has extra budget, you can view the new scholarship award winners !

There is twice more winners now, 2 at each semester :

```sh
 python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT student_name, domain, semester, scholarship_amount FROM analytics.scholarship_award WHERE domain = \'Economics\'').df())"
```

```
    student_name     domain    semester  scholarship_amount
0  Lauren Levine  Economics  Semester 2                1000
1  Gabriel Cooke  Economics  Semester 2                 500
2   Daniel Lopez  Economics  Semester 1                1000
3  Gabriel Cooke  Economics  Semester 1                 500
```

As you can see now, the expenses have doubled :

```sh
lea run --select analytics.finance.expenses
python -c "import duckdb; print(duckdb.connect('school_max.db').execute('SELECT total_expenses FROM analytics.finance__expenses').df())"
```

```
   total_expenses
0         24000.0

```
