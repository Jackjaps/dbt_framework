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
stg_details_available_dt as (
	select 
	item_key,detail,
	trim(substring(detail_value,1,POSITION(' ' in detail_value))) as day,
	trim(substring(detail_value, POSITION(' ' in detail_value),length(detail_value)-6)) as month ,
	trim(substring(detail_value,length(detail_value)-4,length(detail_value))) as year	
	from {{ ref('staging_details') }}
	where detail = ('Producto en Amazon.com.mx desde')
),
available_date as ( 
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
	end as available_month
	from stg_details_available_dt
),
staging_details_available_date as (
	select 
	item_key,
	to_date(concat(day,' ',available_month,' ',year),'dd month yyy') as available_dt
	from available_date
)

select * from staging_details_available_date