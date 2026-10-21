\echo '--- service tables (public): what the quality bridge stored'
select dataset_id, count(*) as batches, sum(records) as records from batches group by dataset_id order by dataset_id;
select dataset_id, scope_id as batch, status, decided_at::timestamp(0) from quality_decisions order by dataset_id, scope_id;
\echo '--- raw layer (schema raw): loaded by the Airflow DAG, one row set per batch'
select 'orders' as table_name, count(*) as rows, count(distinct _batch_id) as batches from raw.orders
union all select 'order_items', count(*), count(distinct _batch_id) from raw.order_items
union all select 'order_payments', count(*), count(distinct _batch_id) from raw.order_payments
union all select 'customers', count(*), count(distinct _batch_id) from raw.customers;
\echo '--- analytics layer (schema analytics): built by dbt'
select 'fct_orders' as model, count(*) as rows from analytics.fct_orders
union all select 'revenue', count(*) from analytics.revenue;
select to_char(day, 'YYYY-MM') as month, sum(orders) as orders, round(sum(revenue)) as revenue
from analytics.revenue group by 1 order by 1;
