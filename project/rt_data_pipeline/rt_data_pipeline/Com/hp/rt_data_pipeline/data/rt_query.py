def extracting_from_bigquery(bigquery_client,export_id,property):
    query=f'''SELECT distinct export_id,Product_Category,Date,Device_Category,Product_Detail_Views,Country,
        ProductSKU,Product_Name,Product_Adds_to_Cart,Quantity_Added_to_Cart,Product_Removes_From_Cart,
        Quantity_Removed_From_cart,view_date 
        from `ww-ga360-bigquery-stellar-bi.MVP_New.MVP_2_7_intermediate_bq_{property}` 
        where export_id > {export_id} and 
        export_id = (select MAX(export_id) from `ww-ga360-bigquery-stellar-bi.MVP_New.MVP_2_7_intermediate_bq_{property}`)'''
    data = execute_query(bigquery_client,query)
    return data

def exporting_max_export_id(cursor,conn):
    query='''select MAX(export_id) from public.MVP_2_7_current_day_intermediate_final'''
    cursor.execute(query)
    a=cursor.fetchall()
    export_id=int(a[0][0])
    conn.commit()
    return export_id

def truncating_intermediate_table(cursor,conn):
    query='''truncate table public.MVP_2_7_current_day_intermediate_final'''
    query_1="COPY public.MVP_2_7_current_day_intermediate_final FROM STDIN WITH (FORMAT 'csv', DELIMITER ',')"
    query_2='''select count(*) from public.MVP_2_7_current_day_intermediate_final'''
    cursor.execute(query)
    conn.commit()
    with open("MVP_2_7_latest_export.csv",'r',encoding='UTF-8') as f:
        cursor.copy_expert(query_1, f)
    cursor.execute(query_2)
    count_mvp=cursor.fetchall()
    count_mvp1=int(count_mvp[0][0])
    conn.commit()
    return count_mvp1

def deleting_old_data(cursor,conn):
    query='''delete from public.MVP_2_7_current_day_final p
	        where EXISTS 
            (select 1 from public.MVP_2_7_current_day_intermediate_final e 
            where e."Date" = p."Date" 
            and e.Country= p.Country and
            e.Product_Category=p.Product_Category and e.ProductSKU=p.ProductSKU and e.product_name=p.product_name
            and e.Device_Category=p.Device_Category and e.view_date=p.view_date)'''
    cursor.execute(query)
    conn.commit()

def inserting_new_rows(cursor,conn):
    query='''insert into public.MVP_2_7_current_day_final
             select "product_category","Date","device_category",
             "product_detail_views", "country","productsku",
             "product_name","product_adds_to_cart","quantity_added_to_cart",
             "product_removes_from_cart", "quantity_removed_from_cart",view_date 
             from public.MVP_2_7_current_day_intermediate_final'''
    cursor.execute(query)
    conn.commit()

def deleting_past_data(cursor,conn,country):
    query=''' select MAX("Date") from public.MVP_2_7_current_day_final where country='{}' '''.format(country)
    cursor.execute(query)
    Date=cursor.fetchall()
    Date=int(Date[0][0])
    query_1='''delete from public.MVP_2_7_current_day_final where country='{}' and "Date" < %(int)s '''.format(country)
    cursor.execute(query_1,{'int':Date})
    conn.commit()

def execute_query(bigquery_client,query):
    query_job = bigquery_client.query(query)
    data = query_job.result()
    data = data.to_dataframe()
    return data
def execute_query_postgre(cursor,conn,query):
    cursor.execute(query)
    conn.commit()



    