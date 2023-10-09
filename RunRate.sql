/* Run Rate estimates future performance based on current trends, assuming they will continue 

Calculation is based on this dataset https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database */


-------------- 1. Modifcation of the Actual Sales data ----------
/* The actual sales table (sales.orders) is transformed to create a consistent working schedule, excluding weekends and public holidays. 
The Run Rate calculation assumes the stores operate from Monday to Friday, excluding weekends and holidays.	
Run Rate calculation starts at row 187
*/
 

-- 1.1. Calendar with weekends and the US holidays

		drop table if exists Calendar

		create table Calendar (
			Date date,					
			WeekdayName varchar(20),	-- for excluding Saturdays and Sundays from Run Rate as there are no sales during the weekend 
			IsHoliday bit)				-- for excluding holidays from Run Rate as there are no sales during the holidays

		declare @StartDate date = '2016-01-01';
		declare @EndDate date = '2017-12-31';

		while @StartDate <= @EndDate
		begin
			insert into Calendar (Date, WeekdayName, IsHoliday)
			values (@StartDate
				  , datename(weekday, @StartDate)
				  , case when @StartDate in ('2016-01-01', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-25', '2017-01-02', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25')	
					then 1 else 0 end)   -- the US holidays

			set @StartDate = dateadd(day, 1, @StartDate)
		end


-- 1.2. Excluding weekends and holidays from sales.orders
-- 1.2.1. Weekends and holidays in sales.orders

		drop table if exists #DatesToExclude

		select distinct s.order_date, 
						dense_rank() over(order by s.order_date) as rnk
		into #DatesToExclude
		from sales.orders s
		
		inner join calendar c
			on c.date = s.order_date
			and ( c.WeekdayName in ('Saturday', 'Sunday')
				  or
				  c.Date in ('2016-01-01', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-25', 
							'2017-01-02', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25')) -- US holidays
		
		where s.order_date < '01-01-2018'
	

-- 1.2.2. Working days to substitute the weekends and holidays for

		drop table if exists #DatesForSubs

		select distinct c.date as dt,
						dense_rank() over(order by c.date) as rnk
		into #DatesForSubs
		from Calendar c
		
		left join sales.orders s
			on s.order_date = c.date
		
		where WeekdayName not in ('Saturday', 'Sunday') 
			  and c.Date not in ('2016-01-01', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-25', 
								 '2017-01-02', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25')	

-- 1.2.3. Final table with initial dates and final dates
		
		drop table if exists #Final
		
		select distinct a.order_date as int_d, 
						b.dt as fin_d
		into #Final
		from #DatesToExclude a

		left join #DatesForSubs b
			  on b.rnk = a.rnk


-- 1.2.4. Sales table without weekends and holidays
		
		drop table if exists #Sales1
		
		select * 
		into #Sales1
		from sales.orders

		update #Sales1
		set order_date = isnull(fin_d, order_date)
		from #Sales1
		
		left join #Final 
			on order_date = int_d

/* 1.2.5. Check (should return 0 rows)
		select * 
		from #Sales1 o

		left join Calendar c
			on c.Date = o.order_date
		
		where c.WeekdayName in ('Saturday', 'Sunday') */


-- 1.3. Add missing dates to sales.orders
-- 1.3.1. Finding dates to add

		drop table if exists #DatesToAdd

		select distinct c.date as dt, 
						dense_rank() over(order by c.date) as rnk
		into #DatesToAdd
		from Calendar c

		left join #Sales1 s
			on s.order_date = c.date
		
		where 1=1
		      and s.order_date is null 
			  and WeekdayName not in ('Saturday', 'Sunday') 
			  and c.Date not in ('2016-01-01', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-25', 
								 '2017-01-02', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25')	

		drop table if exists #Sales2

		select * 
		into #Sales2
		from(
			select n.*, 
				   dense_rank() over(order by n.order_date) as rnk
			from #Sales1 n) n
			where n.rnk <= (select max(rnk) from #datestoadd)


-- 1.3.2. Sales table with additional working dates

		update #Sales2
		set order_date = dt
		from #Sales2 n
		
		left join #DatesToAdd d
			on n.rnk = d.rnk


		drop table if exists #Sales22

		select * 
		into #Sales22 from #Sales2

		alter table #Sales22
		drop column rnk 

-- 1.4. Union
		
		drop table if exists sales.orders_new

		select * 
		into sales.orders_new
		from(
			select * from #Sales1
			union all
			select * from #Sales22) n

/* 1.5. Check (should return 0 rows)

		select * 
		from Calendar c
		
		left join sales.orders_new s
			on s.order_date = c.date
		
		where 1=1
			  and s.order_date is null 
			  and WeekdayName not in ('Saturday', 'Sunday') 
			  and c.Date not in ('2016-01-01', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-25', 
							     '2017-01-02', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25') */




--------------- 2. Run Rate ---------------
-- 2.1. Actual Sales by Stores for Runrate
		drop table if exists #FactPrep1

		select  o.order_date
			  , sr.store_name as Store
			  , round(cast(sum(oi.list_price * oi.quantity * (1 - oi.discount)) as float),0) as OrderAmount	
			  , round(cast(sum(case when o.order_status = 4 then 
						oi.list_price * oi.quantity * (1 - oi.discount) end) as float), 0) as CompletedOrderAmount
		into #FactPrep1
		from sales.orders_new o

		left join sales.order_items oi
			 on oi.order_id = o.order_id

		left join sales.stores sr
			on sr.store_id = o.store_id

		group by o.order_date	  
			   , sr.store_name


		drop table if exists #FactPrep2

		select  eomonth(fp.order_date) as Month
			  , sum(fp.OrderAmount) as OrderAmount
			  , sum(fp.CompletedOrderAmount) as CompletedOrderAmount
			  , fp.Store
		into #FactPrep2
		from #FactPrep1 fp

		group by  eomonth(fp.order_date)
				, fp.Store



-- 2.2. Getting the latest sales data to base Run Rate calculation on it
-- Note: Run rate predicts the future. If a financial year ends in December, it's not useful in that month. So, the data is cut to May to simulate the year-end run rate

		drop table if exists #MaxFact

		select * 
		into #MaxFact
		from (
				select  fp1.order_date
					  , fp1.Store
					  , fp1.CompletedOrderAmount
					  , sum(fp1.CompletedOrderAmount) over(partition by fp1.Store,year(fp1.order_date)  order by fp1.order_date) as CompletedOrderAmountRT_Y  -- actual sales Running Total from the beginning of the year
					  , sum(fp1.CompletedOrderAmount) over(partition by fp1.Store, eomonth(fp1.order_date)  order by fp1.order_date) as CompletedOrderAmountRT_M -- actual sales Running Total from the beginning of the month
					  , max(order_date) over(partition by fp1.Store) as Max_dt
				from #FactPrep1 fp1

				where fp1.order_date < '2017-05-15'
					 ) f
		where f.order_date = Max_dt


-- 2.3. Adding columns with past and future days to the calendar
-- Note: Temp tables below calculate for each day the number of this workday and remaining workdays in the current month (for monthly Run Rate) or year (for yearly Run Rate) 

		drop table if exists #RRCalendar1

		select  cl.Date
		      , cl.IsHoliday
			  , cl.WeekdayName
			  , sum(case when cl.IsHoliday = 0 and cl.WeekdayName not in ('Saturday', 'Sunday') then 1 else 0 end)
					 over (partition by eomonth(cl.Date) order by cl.Date) as From_M_start
			  , sum(case when cl.IsHoliday = 0 and cl.WeekdayName not in ('Saturday', 'Sunday') then 1 else 0 end) 
				over (partition by eomonth(cl.Date)) as Max_M
			  , sum(case when cl.IsHoliday = 0 and cl.WeekdayName not in ('Saturday', 'Sunday') then 1 else 0 end)
					 over (partition by year(cl.Date) order by cl.Date) as From_Y_start
			  , sum(case when cl.IsHoliday = 0 and cl.WeekdayName not in ('Saturday', 'Sunday') then 1 else 0 end) 
				over (partition by year(cl.Date)) as Max_Y
		into #RRCalendar1
		from Calendar cl
		order by cl.Date asc

		
		drop table if exists #RRCalendar2

		select  cl.Date
		      , cl.IsHoliday
			  , cl.WeekdayName
			  , cl.From_M_start
			  , cl.Max_M - From_M_start as Till_M_end
			  , cl.From_Y_start
			  , cl.Max_Y - From_Y_start as Till_Y_end
		into #RRCalendar2
		from #RRCalendar1 cl
		order by cl.Date asc


-- 2.4. Calculating Run Rate
-- Note: Run Rate calulation: Total actual sales / past working days * future working days

		drop table if exists #RunRate

		select  c.Date
			  , c.WeekdayName
			  , c.IsHoliday
			  , fp.Store
			  , fp.CompletedOrderAmountRT_Y
			  , fp.CompletedOrderAmountRT_M
			  , c.From_M_start
			  , c.Till_M_end
			  , c.From_Y_start
			  , c.Till_Y_end
			  , round(fp.CompletedOrderAmountRT_Y / c.From_Y_start * c.Till_Y_end, 0) as Year_RunRate
			  , round(fp.CompletedOrderAmountRT_M / c.From_M_start * c.Till_M_end, 0) as Month_RunRate
		into #RunRate
		from #RRCalendar2 c

		left join #MaxFact fp
			on fp.order_date = c.Date

		where fp.order_date is not null
		
		order by fp.Store, c.Date


-- 2.5. Final table with Clean Runrate and Run Rate based forecast
		
		drop table if exists Current_Run_Rate

		select  r.Date
			  , r.Store
			  , r.CompletedOrderAmountRT_M as [Total sales for the current month]
			  , r.Month_RunRate as [Clean Month Run Rate]
			  , r.CompletedOrderAmountRT_M + r.Month_RunRate as [Month Actual and Run Rate (end of the month forecast)]
			  , r.CompletedOrderAmountRT_Y as [Total sales for the current year]
			  , r.Year_RunRate as [Clean Year Run Rate]
			  , r.CompletedOrderAmountRT_Y + r.Year_RunRate as [Year Actual and Run Rate (end of the year forecast)]
		into Current_Run_Rate
		from #RunRate r

		select * from Current_Run_Rate

		    