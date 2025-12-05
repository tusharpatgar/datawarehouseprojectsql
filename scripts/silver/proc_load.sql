INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
SELECT 
[cst_id],
[cst_key],
TRIM([cst_firstname])as [cst_firstname],
TRIM([cst_lastname])as [cst_lastname],
CASE WHEN cst_marital_status='S' THEN 'SINGLE'
	 WHEN [cst_marital_status]='M' THEN 'MARRIED'
	 ELSE 'N/A'
END AS [cst_marital_status],
CASE WHEN [cst_gndr]='M' THEN 'MALE'
	 WHEN [cst_gndr]='F' THEN 'FEMALE'
	 ELSE 'N/A'
END AS [cst_gndr],
[cst_create_date]
FROM
(select 
*,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_LAST
from [bronze].[crm_cust_info]
WHERE cst_id is not null
)t where flag_LAST=1

 INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
select 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
CASE WHEN prd_line='M' THEN 'Mountain'
	 WHEN prd_line='R' THEN 'Roads'
	 WHEN prd_line='S' THEN 'Other Roads'
	 WHEN prd_line='T' THEN 'Touring'
	 ELSE 'N/A'
END as prd_line,
CAST(CAST(prd_start_dt as varchar) as DATE) as prd_start_dt,
CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
		AS DATE
) AS prd_end_dt
 from [bronze].[crm_prd_info]
