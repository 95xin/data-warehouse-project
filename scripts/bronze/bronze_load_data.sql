CREATE OR REPLACE PROCEDURE bronze.import_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 
    COPY bronze.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
	FROM '/tmp/source_crm/cust_info.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.crm_cust_info successfully';

    -- 
    COPY bronze.crm_prd_info(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt) 
	FROM '/tmp/source_crm/prd_info.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.crm_prd_info successfully';

    -- 
    COPY bronze.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales,sls_quantity,sls_price) 
	FROM '/tmp/source_crm/sales_details.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.crm_sales_datails successfully';


	COPY bronze.erp_cust_az12(cid, bdate, gen) 
	FROM '/tmp/source_erp/CUST_AZ12.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.erp_cust_az12 successfully';

	COPY bronze.erp_loc_a101(cid, cntry) 
	FROM '/tmp/source_erp/LOC_A101.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.erp_loc_a101 successfully';

	COPY bronze.erp_px_cat_g1v2(id,cat,subcat,maintenance) 
	FROM '/tmp/source_erp/PX_CAT_G1V2.csv' 
	DELIMITER ',' 
	CSV HEADER;
    
    RAISE NOTICE 'Data imported into bronze.erp_px_cat_g1v2 successfully';
    

END;
$$;


CALL bronze.import_data();
