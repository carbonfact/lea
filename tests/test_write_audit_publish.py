import lea

def test_without_any_errors():

    client = lea.clients.DuckDB(':memory:')
    runner = lea.Runner(
        'examples/wap/the-great-pizza-cleanup/views',
        client
    )
    runner.prepare()
    runner.run(
        select=[],
        freeze_unselected=False,
        print_views=False,
        dry=False,
        fresh=True,
        threads=4,
        show=20,
        fail_fast=True,
        wap_mode=True,
    )

    tables = client.con.execute('SHOW ALL TABLES').fetchdf()
    table_references = set(tables['schema'].str.cat(tables['name'], sep='.'))
    assert table_references == {
        'analytics.kpis',
        'analytics.peak_hours',
        'analytics.sales_by_month',
        'analytics.top10_bestseller',
        'pizza_police.orders',
        'pizza_police.pizze',
        'raw.order_details',
        'raw.orders',
        'raw.pizza_types',
        'raw.pizzas',
        'staging.orders',
        'staging.pizza_ingredients',
        'staging.pizzas',
    }
