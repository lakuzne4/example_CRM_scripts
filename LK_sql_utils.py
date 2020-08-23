from imports import *
from sqlalchemy import MetaData,Table,Column,Integer,String,create_engine,func,text,select,cast,DATETIME,and_,outerjoin,distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import TIMESTAMP,INT,BIGINT,DECIMAL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import StaticPool

import datetime

#изменения
import sqlite3
from sqlalchemy.pool import StaticPool

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
    
def e(query_text):
    #conn=conn_func()
    print('{:>20} {}'.format(datetime.datetime.now(),query_text))
    return execute_sql(query_text,conn)
    
def invalidate(table):
    pass
    
def create_table_from_query(inp_class,s2,table_name=None):
    if table_name is None:
        table_name='sbx_005.lk_del'+str(inp_class.table_count)
        
    table_query=f"""CREATE TABLE {table_name} AS {str(s2.compile(compile_kwargs={'literal_binds':True}))}"""
    
    e(f"""DROP TABLE IF EXISTS {table_name}""")
    e(table_query)
    print(f"""{table_name} создана""")
    
    inp_class.table_count+=1

    inp_class.список_таблиц.append((inp_class.table_count,table_name,table_query))
    res=assign_tab(table_name,schema='sbx_005')
    return res


# законнектить engine
def conn_func():
    conn=sqlite3.connect('sbx_005.db',)

    # две схемы
    conn.execute("attach 'sbx_005.db' as sbx_005")
    conn.execute("attach 'client_data_mart.db' as client_data_mart")
    conn.execute("attach 'etl_data_mart.db' as etl_data_mart")


    return conn
    
conn=conn_func()
engine = create_engine("sqlite://", poolclass=StaticPool, creator=lambda: conn)
mt=MetaData(bind=engine)
Base=declarative_base(metadata=engine)

def cmp(s,ress=False):
    res=str(s.compile(compile_kwargs={'literal_binds':True}))
    if ress:
        return res
        
def assign_tab(table_name,schema):
    error_limit=0;retry=True;
    while error_limit<5 and retry==True:
        try:
            input_tab=table_name.split('.')[-1]
            res=Table(input_tab,mt,schema=schema,autoload=True,autoload_with=engine)
            retry=False
        except DBAPIError as exc:
            res=0
            retry=True
            error_limit+=1
    if res==0:
        print('Ошибка с DBAPI: нельзя найти таблицу')
        raise AssertionError
    return res
        
        


