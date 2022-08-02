-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
    format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month, 
    count(totals.visits) as visits, sum(totals.pageviews) as pageviews, 
    sum(totals.transactions) as transactions, 
    sum(totals.totaltransactionRevenue)/ power(10,6) as revenue
from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where 
    _table_suffix between '20170101' and '20170331'
group by month
order by month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
    trafficSource.source as source, 
    count(totals.visits) as total_visits, 
    count(totals.bounces) as total_no_of_bounces, 
    count(totals.bounces) / count(totals.visits) * 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by source
order by total_visits desc

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with Week_type as(
    SELECT
        "Week" as time_type,
        format_date("%Y%W", parse_DATE("%Y%m%d", date)) as time,
        trafficsource.source,
        sum(totals.totalTransactionRevenue) as revenue
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by time, trafficsource.source
),
Month_type as (
    SELECT
        "Month" as time_type,
        format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
        trafficsource.source,
        sum(totals.totalTransactionRevenue) as revenue
FROM 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by month, trafficsource.source
)
select 
    * 
from Week_type
union all
select 
    * 
from Month_type
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with cal_purchaser as(
    select
        format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
        sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_purchase
from
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where
  _table_suffix between '20170601' and '20170731'
  and totals.transactions >=1
group by month
),
cal_non_purchaser as(
    select
        format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
        sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_non_purchase
from
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where
  _table_suffix between '20170601' and '20170731'
  and totals.transactions is null
group by month
)
select *
from cal_purchaser join cal_non_purchaser using (month)
order by month



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select 
   format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month, 
   sum(totals.transactions) / count(distinct fullVisitorId)
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  where totals.transactions >= 1
  group by month


-- Query 06: Average amount of money spent per session
#standardSQL
select 
  format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month, 
  sum(totals.totalTransactionRevenue) / count(totals.visits) as avg_revenue_by_user_per_visit
from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where 
    totals.transactions is not null
group by month




-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with user_purchased_Youtube as (
  select distinct
  fullVisitorId,
from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    cross join unnest(hits) as hits
    cross join unnest(hits.product) as product
where 
    v2ProductName = "YouTube Men's Vintage Henley"
    and productRevenue is not null
)
select distinct 
    v2ProductName as other_purchased_products,
    sum(productQuantity) as quantity
from 
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    cross join unnest(hits) as hits
    cross join unnest(hits.product) as product
    right join user_purchased_Youtube using (fullVisitorId)
where 
    productRevenue is not null
    and v2ProductName not like '%Vintage Henley%'
group by 
    v2ProductName



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with element as(
select
  format_date("%Y%m", parse_DATE("%Y%m%d", date)) as month,
   sum(case when hits.eCommerceAction.action_type = '2' then 1 else 0 end) as num_product_view,
   sum(case when hits.eCommerceAction.action_type = '3' then 1 else 0 end) as num_addtocart,
   sum(case when hits.eCommerceAction.action_type = '6' then 1 else 0 end) as num_purchase
from
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  cross join unnest(hits) as hits
where
  _table_suffix between '0101' and '0331'
group by month
)
select
  *, 
  num_addtocart/ num_product_view *100 as add_to_cart_rate,
  num_purchase / num_product_view *100 as purchase_rate
from 
  element
order by element.month
