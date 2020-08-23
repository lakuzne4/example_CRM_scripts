
from imports import pd,np,plt,sns,display,HTML

def execute_sql(query_text,conn):
    cursor=conn.cursor()
    cursor.execute(query_text)
    if cursor.description is not None:
        result=pd.DataFrame(cursor.fetchall(),columns=[d[0] for d in cursor.description])
        cursor.close()
        return result
    else:
        cursor.close()
        return None

import sqlite3

conn=sqlite3.connect('sbx_005.db')

def e(query_text):
    return execute_sql(query_text,conn)
	
	
e("""CREATE TABLE dolphin_request (
product_nm STRING,
customer_id INT,
request_id INT,
request_dt TIMESTAMP,
issue_id INT,
issue_dt TIMESTAMP,
start_dt TIMESTAMP,
issue_sum FLOAT);""")
e("""CREATE TABLE casatka (
product_name STRING,
customer_id INT,
request_id INT,
request_dt TIMESTAMP,
issue_id INT,
issue_dt TIMESTAMP,
start_dt TIMESTAMP,
issue_sum FLOAT);""")
e("""CREATE TABLE pills_request (
product_type STRING,
gold_customer_id INT,
pills_request_id INT,
request_dt TIMESTAMP,
issue_id INT,
issue_dt TIMESTAMP,
start_dt TIMESTAMP,
issue_sum FLOAT);""")
e("""CREATE TABLE gepard_request (
depo_product STRING,
customer_id INT,
depo_request_id INT,
request_dt TIMESTAMP,
issue_id INT,
issue_dt TIMESTAMP,
start_dt TIMESTAMP,
issue_sum FLOAT);""")
e("""CREATE TABLE monkey_request (
product_nm STRING,
customer_id INT,
mrtg_request_id INT,
request_dt TIMESTAMP,
issue_id INT,
issue_dt TIMESTAMP,
start_dt TIMESTAMP,
issue_sum FLOAT);""")


e("""CREATE TABLE lk_camp_dict
(
campaign_nm STRING,
product_cd STRING,
crm_prod_nm STRING
)

""")

conn=sqlite3.connect('etl_data_mart.db')

e("""CREATE TABLE cdm_contact_history 
(channel_cd STRING,
contact_id INT,
contact_history_status_cd STRING,
contact_dttm TIMESTAMP,
ctrl_grp_flg INT,
customer_id INT,
delivery_status STRING,
participant_id INT);

""")

e("""CREATE TABLE cdm_contact_x_offer 
(contact_id INT,
offer_id INT,
campaign_cd STRING
)""")

e("""CREATE TABLE cdm_customer_reactions 
(
offer_id INT,
gold_customer_id INT,
agree_resp INT,
think_resp INT,
delay_resp INT,
sounded_resp INT,
decline_resp INT
)
""")

e("""CREATE TABLE cdm_ma_product_dict
(
product_cd STRING,
product_name STRING 
)""")

e("""CREATE TABLE cdm_ma_offer
(
offer_id INT,
participant_id INT,
product_cd STRING

)

""")

e("""CREATE TABLE cdm_ci_campaign
(campaign_cd STRING,
campaign_nm STRING)

""")



conn=sqlite3.connect('client_data_mart.db')

e("""CREATE TABLE main_client_register 
(
gold_customer_id INT,
branch_entity_id INT)

""")
e("""CREATE TABLE branch_dict 
(
branch_id INT,
branch_nm STRING
)
""")




