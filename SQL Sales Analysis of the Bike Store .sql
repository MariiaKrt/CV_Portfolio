/* based on this dataset https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database */

-------------------- 1. Data Consistency Check ----------------------

declare @InconsistencyCustomer int, 
	@InconsistencyStaff int,
	@InconsistencyStore int,
	@InconsistencyOrder int;

-- 1.1. Customer_id
		-- Checking missing customer_id values
		select @InconsistencyCustomer = count(distinct o.customer_id)
		from sales.orders o
		where o.customer_id not in (
			select distinct customer_id from sales.customers);

		-- If inconsistencies are found, send an email notification
		if @InconsistencyCustomer > 0
		begin
			exec msdb.dbo.sp_send_dbmail
				@profile_name = 'ProfileName',
				@recipients = 'mail@mail.com', 
				@subject = 'Data Inconsistencies Found',
				@body = 'Missing customer_id(s) in sales.orders: SELECT COUNT(DISTINCT o.customer_id) FROM sales.orders o WHERE o.customer_id NOT IN (SELECT DISTINCT customer_id FROM sales.customers)'
		end

-- 1.2. Staff_id
		-- Checking missing staff_id values
		select @InconsistencyStaff = count(distinct o.staff_id)
		from sales.orders o
		where o.staff_id not in (
			select distinct staff_id from sales.staffs);

		-- If inconsistencies are found, send an email notification
		if @InconsistencyStaff > 0
		begin
			exec msdb.dbo.sp_send_dbmail
				@profile_name = 'ProfileName',
				@recipients = 'mail@mail.com', 
				@subject = 'Data Inconsistencies Found',
				@body = 'Missing staff_id(s) in sales.orders: SELECT COUNT(DISTINCT o.staff_id) FROM sales.orders o WHERE o.staff_id NOT IN (SELECT DISTINCT staff_id FROM sales.staffs)'
		end

-- 1.3. Store_id
		-- Checking missing store_id values
		select @InconsistencyStore = count(distinct o.store_id)
		from sales.orders o
		where o.store_id not in (
			select distinct store_id FROM sales.stores);

		-- If inconsistencies are found, send an email notification
		if @InconsistencyStore > 0
		begin
			exec msdb.dbo.sp_send_dbmail
				@profile_name = 'ProfileName',
				@recipients = 'mail@mail.com', 
				@subject = 'Data Inconsistencies Found',
				@body = 'Missing store_id(s) in sales.orders: SELECT COUNT(DISTINCT o.store_id) FROM sales.orders o WHERE o.store_id NOT IN (SELECT DISTINCT store_id FROM sales.stores);'
		end

-- 1.4. Order_id
		-- Checking missing order_id values in sales.order_items
		select @InconsistencyOrder = count(distinct oi.order_id)
		from sales.orders oi
		where oi.order_id not in (
			select distinct order_id from sales.order_items);

		-- If there are any inconsistencies, send an email notification
		if @InconsistencyOrder > 0
		begin
			exec msdb.dbo.sp_send_dbmail
				@profile_name = 'ProfileName',
				@recipients = 'mail@mail.com',
				@subject = 'Data Inconsistencies Found',
				@body = 'Missing order_id(s) in sales.orders: SELECT COUNT(DISTINCT o.order_id) FROM sales.orders o WHERE oi.order_id NOT IN (SELECT DISTINCT order_id FROM order_items);'
		end

-- 1.5. Result
		-- The script will not execute if there are inconsistencies
		if  @InconsistencyCustomer + @InconsistencyStaff + @InconsistencyStore + @InconsistencyOrder > 0 
		begin
			return;
		end;

-------------------- End of Data Consistency Check ----------------------


------------------------ 2. Start of Calculation -------------------------

-- 2.1. Prep tables
-- 2.1.1. Calculating Order Amounts and Number of Products in each order

		drop table if exists #OrdersPrep

		select  oi.order_id
			  , round(cast(sum(oi.list_price * oi.quantity * (1 - oi.discount)) as float),0) as OrderAmount	
			  , count (distinct oi.product_id) as ProductsInOrder
		into #OrdersPrep
		from sales.order_items oi 

		group by oi.order_id


-- 2.1.2. Calculating Top performing Product, Store, Staff

		drop table if exists #TopSalesPrep

		select  eomonth(o.order_date) as OrderMonth
			  , concat(st.first_name, ' ', st.last_name) as ManagerName
			  , concat(sr.store_name, ' (', sr.state, ')') as StoreName
			  , concat(p.product_name, ' (', b.brand_name, ')') as ProductNameAndBrand
			  , round(cast(sum(oi.list_price * oi.quantity * (1 - oi.discount)) as float),0) as OrderAmount
		into #TopSalesPrep
		from sales.orders o

		left join sales.order_items oi
			 on oi.order_id = o.order_id

		left join production.products p
			on p.product_id = oi.product_id

		left join production.brands b
			on b.brand_id = p.brand_id

		left join sales.staffs st
			on st.staff_id = o.staff_id

		left join sales.stores sr
			on sr.store_id = o.store_id

		where o.order_status = 4  -- Order status: 1 = Pending; 2 = Processing; 3 = Rejected; 4 = Completed

		group by  eomonth(o.order_date)
				, concat(st.first_name, ' ', st.last_name)
				, concat(sr.store_name, ' (', sr.state, ')')
				, concat(p.product_name, ' (', b.brand_name, ')')

------- Top Managers

		drop table if exists #TopManagersPrep

		select  t.OrderMonth
			  , t.ManagerName
			  , sum(t.OrderAmount) as OrderAmount
			  , rank() over (partition by t.OrderMonth order by sum(t.OrderAmount) desc) as Rnk
		into #TopManagersPrep
		from #TopSalesPrep t

		group by  t.OrderMonth
			    , t.ManagerName

		order by  t.OrderMonth 
				, sum(t.OrderAmount) desc

------- Top Stores

		drop table if exists #TopStoresPrep

		select  t.OrderMonth
			  , t.StoreName
			  , sum(t.OrderAmount) as OrderAmount
			  , rank() over (partition by t.OrderMonth order by sum(t.OrderAmount) desc) as Rnk 
		into #TopStoresPrep
		from #TopSalesPrep t

		group by  t.OrderMonth
		        , t.StoreName

		order by  t.OrderMonth
		        , sum(t.OrderAmount) desc

------- Top Products

		drop table if exists #TopProductsPrep

		select  t.OrderMonth
			  , t.ProductNameAndBrand
			  , sum(t.OrderAmount) as OrderAmount
			  , rank() over (partition by t.OrderMonth order by sum(t.OrderAmount) desc) as Rnk 
		into #TopProductsPrep
		from #TopSalesPrep t

		group by  t.OrderMonth
			    , t.ProductNameAndBrand

		order by  t.OrderMonth
		        , sum(t.OrderAmount) desc


-- 2.1.3. Calculating Lost Customers and Delayed Orders
-- Note: Order status: 1 = Pending; 2 = Processing; 3 = Rejected; 4 = Completed

		drop table if exists #LostCustOrders

		select  eomonth(o.order_date) as OrderMonth
			  , o.order_id
			  , o.customer_id
			  , o.order_date
			  , o.order_status
			  , o.required_date
			  , o.shipped_date
			  , o.staff_id
			  , o.store_id
			  , case when max(o.order_status) over (partition by o.customer_id, eomonth(o.order_date)) = 3
						  and min(o.order_status) over (partition by o.customer_id, eomonth(o.order_date)) = 3
					 then o.customer_id end as LostCustomers	 
			  , case when o.required_date < o.shipped_date then o.order_id end as DelayedOrders	  
		into #LostCustOrders
		from sales.orders o


-- 2.1.4. Calculating Main Metrics

		drop table if exists #MonthlyDataPrep1

		select  eomonth(o.order_date) as OrderMonth
			  , sum(case when o.order_status = 4 then ord.OrderAmount end) as CompletedSalesAmount				-- Sum of sales with order status = 'Completed'
			  , sum(case when o.order_status in (1,2) then ord.OrderAmount end) as PendingSalesAmount			-- Sum of sales with order status = 'Pending' or 'Processing'
			  , sum(case when o.order_status = 3 then ord.OrderAmount end) as LostSalesAmount				-- Sum of sales with order status = 'Rejected'
			  , avg(case when o.order_status = 4 then ord.OrderAmount end) as AverageCompletedOrderAmount			-- Average order amount with order status = 'Completed'
			  , min(case when o.order_status = 4 then ord.OrderAmount end) as MinCompletedOrderAmount			-- Min order amount with order status = 'Completed'
			  , max(case when o.order_status = 4 then ord.OrderAmount end) as MaxCompletedOrderAmount			-- Max order amount with order status = 'Completed'
			  , count(distinct case when o.order_status = 4 then o.order_id end) as NumCompletedOrders			-- Number of completed orders
			  , count(distinct case when o.order_status = 3 then o.order_id end) as NumLostOrders				-- Number of rejected orders
			  , count(distinct case when o.order_status = 4 then o.customer_id end) as NumCustomersWithOrders		-- Number of customers with completed orders
			  , count(distinct LostCustomers) as LostCustomers								-- Number of customers with only rejected orders
			  , count(distinct prev.customer_id) as RetainedCustomers							-- Customers with previous completed orders who placed orders this month
			  , count(distinct DelayedOrders) as DelayedOrders								-- Orders that were shipped after the required delivery date
		into #MonthlyDataPrep1
		from #LostCustOrders o

		left join #OrdersPrep ord
			   on o.order_id = ord.order_id

		left join sales.orders prev 
			   on prev.customer_id = o.customer_id
			   and prev.order_status = 4
			   and o.order_id <> 3
			   and format(prev.order_date, 'yyyy-MM') < format(o.order_date, 'yyyy-MM')	
	   
		group by eomonth(o.order_date)



		drop table if exists #MonthlyDataPrep2

		select  p1.OrderMonth
			  , p1.CompletedSalesAmount
			  , p1.PendingSalesAmount
			  , p1.LostSalesAmount
			  , round(cast(p1.AverageCompletedOrderAmount as float),0) as AverageCompletedOrderAmount
			  , p1.MinCompletedOrderAmount
			  , p1.MaxCompletedOrderAmount
			  , p1.NumCompletedOrders
			  , p1.NumLostOrders
			  , p1.NumCustomersWithOrders
			  , p1.LostCustomers
			  , p1.RetainedCustomers
			  , p1.DelayedOrders
			  , case when (isnull(p1.CompletedSalesAmount,0) + isnull(p1.PendingSalesAmount,0)) > 0 
					 then round(cast(p1.LostSalesAmount as float) / (isnull(p1.CompletedSalesAmount,0) + isnull(p1.PendingSalesAmount,0)),2) end as PercentOfLostSales
			  , round (cast(p1.CompletedSalesAmount as float) /
					   case when dense_rank() over(order by p1.OrderMonth asc) > 3
							then avg(p1.CompletedSalesAmount) over (order by p1.OrderMonth rows between 3 preceding and 1 preceding) end, 2) as PercentofLast3MonthAv 
			  , sum(p1.CompletedSalesAmount) over (partition by year(p1.OrderMonth) order by p1.OrderMonth asc) as RTCompletedSales
		into #MonthlyDataPrep2
		from #MonthlyDataPrep1 p1



		drop table if exists #MonthlyDataPrep3

		select  p2.OrderMonth																																																
			  , p2.CompletedSalesAmount as [Completed Sales Amount]								-- Sum of sales with order status = 'Completed'																				
			  , p2.PendingSalesAmount as [Pending Sales Amount]								-- Sum of sales with order status = 'Pending' or 'Processing'
			  , p2.LostSalesAmount as [Lost Sales Amount]									-- Sum of sales with order status = 'Rejected'
			  , p2.PercentOfLostSales as [% Of Lost Sales]									-- % of lost order amount of the completed, pending, processing order amount
			  , p2.PercentofLast3MonthAv as [Current Month vs Last 3 Month Av]						-- This month's sales compared to the average of the past three months
			  , p2.RTCompletedSales as [Year-to-Date Sales Amount]								-- Cumulative sales from the beginning of the current year until current month
			  , prev2.RTCompletedSales as [Prev Year-to-Date Sales Amount]							-- Previous year's year-to-date sales for the same month
			  , case when isnull(prev2.RTCompletedSales,0) > 0 
					 then round(cast(p2.RTCompletedSales as float) / prev2.RTCompletedSales, 2) 
					 end as [% Year-to-Date Cur Month vs Year-to-Date Prev Year Month]				-- Current year-to-date sales compared to the same period in the previous year
			  , prev2.CompletedSalesAmount as [Prev Year Month]								-- Sales for the same month in the previous year
			  , case when isnull(prev2.CompletedSalesAmount,0) > 0 
					 then round(cast(p2.CompletedSalesAmount as float) / prev2.CompletedSalesAmount, 2) 
					 end as [% Current Month vs Prev Year Month]							-- Comparison of sales between the same months in the previous year and the current year
			  , p2.AverageCompletedOrderAmount as [Average Completed Order Amount]						-- Average order amount with order status = 'Completed'
			  , p2.MinCompletedOrderAmount as [Min Completed Order Amount]							-- Min order amount with order status = 'Completed'
			  , p2.MaxCompletedOrderAmount as [Max Completed Order Amount]							-- Max order amount with order status = 'Completed'
			  , p2.NumCompletedOrders as [# Completed Orders]								-- Number of completed orders
			  , p2.NumLostOrders as [# Lost Orders]										-- Number of rejected orders
			  , p2.DelayedOrders as [# Delayed Orders]									-- Orders that were shipped after the required delivery date
			  , p2.NumCustomersWithOrders as [# Customers With Orders]							-- Number of customers with completed orders
			  , p2.LostCustomers as [# Lost Customers]									-- Number of customers with only rejected orders
			  , p2.RetainedCustomers as [# Retained Customers]								-- Customers with previous completed orders who placed orders this month
			  , tm.ManagerName as [Top Performing Manager]									-- Sales manager with the highest sales amount this month
			  , tp.ProductNameAndBrand as [Top Selling Product (Brand)]							-- Top Selling Product this month
			  , ts.StoreName as [Top Performing Store (State)]								-- Store with the highest sales amount this month
		into #MonthlyDataPrep3
		from #MonthlyDataPrep2 p2

		left join #MonthlyDataPrep2 prev2
			on datepart(year, p2.OrderMonth) - 1 = datepart(year, prev2.OrderMonth)
			and datepart(month, p2.OrderMonth) = datepart(month, prev2.OrderMonth)

		left join #TopManagersPrep tm
			on tm.OrderMonth = p2.OrderMonth
			and tm.Rnk = 1

		left join #TopProductsPrep tp
			on tp.OrderMonth = p2.OrderMonth
			and tp.Rnk = 1

		left join #TopStoresPrep ts
			on ts.OrderMonth = p2.OrderMonth
			and ts.Rnk = 1

		order by p2.OrderMonth

-- 2.1.5. Final Table

		select * from #MonthlyDataPrep3 p3
		where p3.OrderMonth <= '2018-03-31' -- filtered out as there are no completed orders after March 2018
