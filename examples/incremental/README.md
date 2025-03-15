# Incremental scripts

Let's start with creating the database in the usual way:

```sh
echo "
LEA_USERNAME=max
LEA_WAREHOUSE=duckdb
LEA_DUCKDB_PATH=incremental.db
" > .env
```

There are two `scripts` folders to simulate two days with different amounts of data. Let's say we're the 4th of January, and we run our views:

```sh
lea run --scripts scripts_today
```

```sh
python -c "import duckdb; print(duckdb.connect('incremental_max.db').execute('SELECT created_at, day_of_year FROM core.events').df())"
```

```
  created_at  day_of_year
0 2023-01-02            2
1 2023-01-03            3
2 2023-01-04            4
```

The next day, there's new data. When we refresh, we don't want to start from scratch. We want to keep the data from the previous day and only add the new data. This will happen automatically because the view is tagged with a `#INCREMENTAL` comment.

```sh
lea run --script scripts_tomorrow --select core.events --incremental day_of_year 5
```

```sh
python -c "import duckdb; print(duckdb.connect('incremental_max.db').execute('SELECT created_at, day_of_year FROM core.events').df())"
```

```
  created_at  day_of_year
0 2023-01-02            2
1 2023-01-03            3
2 2023-01-04            4
3 2023-01-05            5
```

We can see the new event from the 5th of January. However, in this case there is an event from the 1st of January that is missing. This is because the event has arrived with a delay. In such cases, we can force a full refresh by ommitting the flag --incremental:

```sh
lea run --script scripts_tomorrow
```

```sh
python -c "import duckdb; print(duckdb.connect('incremental_max.db').execute('SELECT * FROM core.events').df())"
```

```
  created_at  day_of_year
0 2023-01-01            1
1 2023-01-02            2
2 2023-01-03            3
3 2023-01-04            4
4 2023-01-05            5
```
