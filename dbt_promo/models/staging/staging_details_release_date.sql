/* 
Author: 
Description: transform date format from "dd month yyy" from spanish months to english months 
Creation Date: 14-may-2023
Domain: 
Details: 
¡¡¡WARNING!!!: NON-Cross database macros found to cast from format "d MONTH YYYY"
*/
{{ config(
    tags=["staging","promotions","details"]
) }}

with 
stg_details_release_dt as (
	select 
	item_key,detail,
	trim(substring(detail_value,1,POSITION(' ' in detail_value))) as day,
	trim(substring(detail_value, POSITION(' ' in detail_value),length(detail_value)-6)) as month ,
	trim(substring(detail_value,length(detail_value)-4,length(detail_value))) as year	
	from {{ ref('staging_details') }}
	where detail = ('Fecha de lanzamiento')
),

release_date as ( 

	select 
	item_key,
	day,
	year,
	case when month = 'enero' then 'january'
	 when month = 'febrero' then 'february'
	 when month = 'marzo' then 'march'
	 when month = 'abril' then 'april'
	 when month = 'mayo' then 'may'
	 when month = 'junio' then 'june'
	 when month = 'julio' then 'july'
	 when month = 'agosto' then 'august'
	 when month = 'septiembre' then 'september'
	 when month = 'octubre' then 'october'
	 when month = 'noviembre' then 'november'
	 when month = 'diciembre' then 'december'
	end as release_month
	from stg_details_release_dt
),

staging_details_release_date as (
	select 
	item_key,
	to_date(concat(day,' ',release_month,' ',year),'dd month yyy') as release_dt
	from release_date
)


select * from staging_details_release_date