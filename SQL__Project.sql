use sql_project;
select *  from fact_sales;
select * from dim_customers;
select * from dim_products;

-- Changes Over Time Analysis (Analyze how a measure evolves over time. Helps track trends and identify seasonality in your data)
with cte as (select year(order_date) as order_year , sum(sales_amount) as total_sales,count(distinct customer_key) as total_customer,sum(quantity) as total_quantity 
from fact_sales where year(order_date) is not null group by year(order_date) order by order_year)
select order_year,total_sales,lag(total_sales,1,total_sales) over(order by order_year) as prv_year,
round((total_sales - lag(total_sales,1,total_sales) over(order by order_year))*100/lag(total_sales,1,total_sales) 
	over(order by order_year),2) as per_change_overyear from cte;
    
-- Cumulative Analysis (Aggregate the data progressively over time. Helps to understand whether our business is growing or declining)
-- Calculate the total sales per month and the running total of sales over time?
with cte as(select date_format(order_date,"%Y-%m-01") as order_date, sum(sales_amount) as total_sales,avg(price) as avg_price from fact_sales 
	where date_format(order_date,"%Y-%m-01") is not null group by date_format(order_date,"%Y-%m-01") order by order_date)

select order_date,total_sales,sum(total_sales) over(partition by year(order_date) order by order_date asc) as running_total_sales,
round(avg(avg_price) over(partition by year(order_date) order by order_date asc),0) as moving_avg_price from cte;

-- Performance Analysis (Comparing the current value to a target value. Helps measure success and compare performance)
-- Q Analyze the yearly performance of products by comparing each product's sales to both its average sales performance and the previous year's sales?

with cte as(select year(f.order_date) as order_year,p.product_name,sum(f.sales_amount) as current_sales from fact_sales as f left join dim_products as p on 
f.product_key=p.product_key where year(f.order_date) is not null group by year(f.order_date),p.product_name order by order_year)

select order_year,product_name,current_sales,ceiling(avg(current_sales) over(partition by product_name)) as avg_sales,
current_sales - ceiling(avg(current_sales) over(partition by product_name)) as diff_avg,case when current_sales - ceiling(avg(current_sales) 
over(partition by product_name))>0 then "Above Avg" when current_sales - ceiling(avg(current_sales) over(partition by product_name)) <0 then "Below Avg"
else "Avg" end as avg_change,lag(current_sales,1,current_sales) over(partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales,1,current_sales) over(partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales,1,current_sales) over(partition by product_name order by order_year) >0 then "Increase" when 
current_sales - lag(current_sales,1,current_sales) over(partition by product_name order by order_year) <0 then "Decrease" else "No Change" end as py_change 
from cte order by product_name,order_year;

-- Part-To-Whole Analysis (Analyze how an individual part is performing compared to the overall, allowing us to understand which category has the greatest impact on the business)
-- Q Which categories contribute the most to overall sales?
with cte as(select p.category,sum(f.sales_amount) as total_sales from fact_sales as f join dim_products as p on f.product_key = p.product_key group by p.category)
select category,total_sales,sum(total_sales) over() as overall_sales,concat(round((total_sales*100/sum(total_sales) over()),2),"%") 
as percentage_of_total from cte order by total_sales desc;

-- Data Segmentation (Group the data based on a specific range. Helps understand the correlation between two measures)
-- Q Segment products into cost ranges and count how many products fall into each segment?

with product_segments as (select product_key,product_name,cost,case when cost <100 then "Below 100" when cost between 100 and 500 then "100-500"
when cost between 500 and 1000 then "500-1000" else "Above 1000" end as cost_range from dim_products)
select cost_range,count(product_key) as total_products from product_segments group by cost_range order by total_products desc;

-- Q Group customers into three segments based on their spending behavior and find the total number of customers by each group?
-- VIP - Customers with at least 12 months of history and spending more than 5,000.
-- Regular - Customers with at least 12 months of history but spending 5,000 or less.
-- New - Customers with a lifespan less than 12 months.

with cte as (select c.customer_key,sum(f.sales_amount) as total_spending,min(f.order_date) as first_order,max(f.order_date) as last_order,
timestampdiff(month,min(f.order_date),max(f.order_date)) as life_span
	from fact_sales as f left join dim_customers as c on f.customer_key=c.customer_key group by c.customer_key),

cte2 as (select customer_key,total_spending,life_span, case when life_span >=12 and total_spending >5000 then "VIP"
when life_span >=12 and total_spending <=5000 then "Regular" else "New" end as customer_segment  from cte)
select customer_segment,count(customer_key) as total_customer from cte2 group by customer_segment order by total_customer desc;


-- Top-N Analysis
-- Q Write an SQL query to find the top 5 customers who have contributed the highest total sales?
with cte as (select customer_key,total_sales from (select customer_key,sum(sales_amount) as total_sales ,dense_rank() over(order by sum(sales_amount) desc) 
as rnk from fact_sales group by customer_key) sal where rnk<=5)

select concat(c.first_name," ",c.last_name) as customer_name,ct.total_sales from cte as ct join dim_customers as c on ct.customer_key=c.customer_key 
order by ct.total_sales desc;



-- Customer Report
-- Purpose : This report consolidates key customer metrics and behaviors
-- Highlights:

-- 1. Gathers essential fields such as names, ages, and transaction details.
-- 2. Segments customers into categories (VIP, Regular, New) and age groups.
-- 3. Aggregates customer-level metrics:
--    total orders
--    total sales
--    total quantity purchased
--    total products
--    lifespan (in months)
-- 4. Calculates valuable KPIs:
--    recency (months since last order)
--    average order value
--    average monthly spend 

-- Solution 

with base_query as (select f.order_number,f.product_key,f.order_date,f.sales_amount,f.quantity,c.customer_key,c.customer_number,concat(c.first_name," ",c.last_name) 
as customer_name, timestampdiff(year,c.birthdate,curdate()) as age from fact_sales 
	as f left join dim_customers as c on f.customer_key=c.customer_key where timestampdiff(year,c.birthdate,curdate()) is not null),
customer_agg as(select customer_key,customer_name,customer_number,age,count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,sum(quantity) as total_quantity,count(distinct product_key) as total_products,
max(order_date) as last_order_date,timestampdiff(month,min(order_date),max(order_date)) as lifespan from base_query 
	group by customer_key,customer_name,customer_number,age)

-- Final Query

select customer_key,customer_name,customer_number,age,case 
when age<20 then "Under 20" when age between 20 and 29 then "20-29"
when age between 30 and 39 then "30-39" 
when age between 40 and 49 then "40-49" else "50 and above" end as age_group,
case when lifespan >=12 and total_sales >5000 then "VIP"
when lifespan >=12 and total_sales <=5000 then "Regular" else "New" end as "customer_segment",last_order_date,timestampdiff(month,last_order_date,curdate()) as recency,
total_orders,total_sales,total_quantity,total_products, lifespan,case when total_sales = 0 then 0 else total_sales/total_quantity end as avg_order_value,
case when lifespan =0 then total_sales else total_sales/lifespan end as avg_monthly_spend from customer_agg;





