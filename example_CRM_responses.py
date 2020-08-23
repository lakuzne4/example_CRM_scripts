from imports import *
from LK_sql_utils import e,invalidate,engine,mt,Base,cmp,assign_tab,create_table_from_query

import os
from collections import OrderedDict
import pdb

from sqlalchemy import MetaData,Table,Column,Integer,String,create_engine,func,text,select,cast,DATETIME,and_,outerjoin
from sqlalchemy.ext.declarative import declarative_base
#from impala.sqlalchemy import TIMESTAMP,DECIMAL,INT,BIGINT
from sqlalchemy.types import TIMESTAMP,INT,DECIMAL,BIGINT

from sqlalchemy.exc import DBAPIError
from sqlalchemy import distinct


class Входные_таблицы:
    #датакласс для входных данных 
    продукты=['dolphin','casatka','pills','gepard','monkey','gepard_saved']
    
    cdm_contact_history=assign_tab('cdm_contact_history',schema='etl_data_mart')
    main_client_register=assign_tab('main_client_register',schema='client_data_mart')
    branch_dict=assign_tab('branch_dict',schema='client_data_mart')
    
    contact_x_offer=assign_tab('cdm_contact_x_offer',schema='etl_data_mart')
    customer_reactions=assign_tab('cdm_customer_reactions',schema='etl_data_mart')
    ma_product_dict=assign_tab('cdm_ma_product_dict',schema='etl_data_mart')
    
    inp_cdm_ma_offer=assign_tab('cdm_ma_offer',schema='etl_data_mart')
    cdm_ma_campaign=assign_tab('cdm_ci_campaign',schema='etl_data_mart')
    
    class Tables_crm:
        def __init__(self):
            condition_for_dolphin="""(
                                upper(t2.PRODUCT_NM) like upper('%малыш%')
                                """
            condition_for_casatka="""(
                                upper(t2.PRODUCT_NM) like upper('%взрослый%')
                                """
            condition_for_gepard="""upper(t2.product_nm)!='НАКОПИТЕЛЬНЫЙ'"""
            condition_for_gepard_saved="""upper(t2.product_nm) ='НАКОПИТЕЛЬНЫЙ'"""
            
            # название таблицы -- имя поля agreement_id
            self.response_tables={'dolphin_request': {'name': 'dolphin_request',
                                                  'product_nm': 'product_nm',
                                                  'client_id_in_response_table': 'customer_id',
                                                  'request_id': 'request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'},
                                                 'casatka': {'name': 'casatka_request',
                                                  'product_nm': 'product_name',
                                                  'client_id_in_response_table': 'customer_id',
                                                  'request_id': 'request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'},
                                                 'pills': {'name': 'pills_request',
                                                  'product_nm': 'product_type',
                                                  'client_id_in_response_table': 'gold_customer_id',
                                                  'request_id': 'pills_request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'},
                                                 'gepard': {'name': f'(SELECT * FROM gepard WHERE {condition_for_gepard})',
                                                  'product_nm': 'gepard_product',
                                                  'client_id_in_response_table': 'customer_id',
                                                  'request_id': 'depo_request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'},
                                                 'mrtg': {'name': 'mrtg_request',
                                                  'product_nm': 'product_nm',
                                                  'client_id_in_response_table': 'customer_id',
                                                  'request_id': 'mrtg_request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'},
                                                 'gepard_saved': {'name': f'(SELECT * FROM gepard_request WHERE {condition_for_gepard_saved})',
                                                  'product_nm': 'depo_product',
                                                  'client_id_in_response_table': 'customer_id',
                                                  'request_id': 'depo_request_id',
                                                  'request_dt': 'request_dt',
                                                  'issue_id': 'issue_id',
                                                  'issue_dt': 'issue_dt',
                                                  'start_dt': 'start_dt',
                                                  'issue_sum': 'issue_sum'}}
            self.prods_batch_tables={key:value for key,value in zip(['dolphin','casatka','pills','gepard','monkey','gepard_saved'],
                                                        ['STAT_batch'+i for i in ['dolphin','casatka','pills','gepard','monkey','gepard_saved']])}
                                                        
                                                        
        def _get_query_for_response_table(self,prod_name:str):
            key=prod_name
            
            #создать запрос к таблице с заявками/выдачами для данного продукта на основе словаря
            res='SELECT '+'\n'
            for attribute in self.response_tables[key]:
                if attribute!='name' and attribute!='start_dt':
                    res+=('t.'+self.response_tables[key][attribute]+' AS '+attribute+'\n')
                elif attribute=='start_dt':
                    res+=('CAST(t.'+self.response_tables[key][attribute]+' AS TIMESTAMP) AS '+attribute+' \n')
            res=res[:-2]
            res+=' \n'
            res+='FROM '+self.response_tables[key]['name']+' t'+'\n'
            return res
            
    таблицы_CRM=Tables_crm()
        
        
        
                                
                                
                                
class Comms():
    # класс для сбора таблиц по коммуникациям/офферам/ответам
    def __init__(self):
        self.CH_cols=['channel_cd','contact_history_status_cd','contact_id',
                'ctrl_grp_flg','customer_id','delivery_status','participant_id']
                
        self.Таблица_запросов={}
        self.список_продуктов=Входные_таблицы.продукты
        
    def addBranchId(self,ВхТабл,CH=None):
        if CH is None:
            CH=ВхТабл.cdm_contact_history
        CH_subquery=select([getattr(CH.c,i) for i in self.CH_cols]+[cast(CH.c.contact_dttm,TIMESTAMP).label('contact_dttm')]).\
                    where(ВхТабл.cdm_contact_history.c.channel_cd.in_(['telegramm','telepathy'])).alias('t1')
        
        GOLD_CUSTOMER_subquery=select([ВхТабл.main_client_register.c.gold_customer_id,
                                        ВхТабл.main_client_register.c.branch_entity_id]).alias('t2')
        
        BRANCH_subquery=select([ВхТабл.branch_dict.c.branch_id,ВхТабл.branch_dict.c.branch_nm]).alias('t3')
        
        CH_cols=['channel_cd','contact_history_status_cd','contact_id',
                'ctrl_grp_flg','customer_id','delivery_status','participant_id'] # повтор self.CH_cols

        j=CH_subquery.join(GOLD_CUSTOMER_subquery,
                            CH_subquery.c.customer_id==GOLD_CUSTOMER_subquery.c.gold_customer_id).\
                            outerjoin(BRANCH_subquery,GOLD_CUSTOMER_subquery.c.branch_entity_id==BRANCH_subquery.c.branch_id)
                            
        self.Таблица_запросов[0]=(select(CH_subquery.c+BRANCH_subquery.c).select_from(j).alias('CH'),'История контактов с ветками')
        
        
    def addOffersAndAnswers(self,s1,ВхТабл):
        #джойн с contact_x_offer
        j=outerjoin(s1,ВхТабл.contact_x_offer,
                    s1.c.contact_id==cast(ВхТабл.contact_x_offer.c.contact_id,DECIMAL(18,0)))
                    
        contacts_with_offers=select([getattr(s1.c,i) for i in [i.name for i in s1.columns]]+\
                                        [cast(ВхТабл.contact_x_offer.c.offer_id,BIGINT).label('offer_id'),
                                        ВхТабл.contact_x_offer.c.campaign_cd]).select_from(j)
                                        
        
        cols_from_customer_reactions=['AGREE_RESP','THINK_RESP','DELAY_RESP','SOUNDED_RESP','DECLINE_RESP']
        cols_from_customer_reactions=[i.lower() for i in cols_from_customer_reactions]
        self.contacts_with_offers=contacts_with_offers
        
        contacts_with_offers=contacts_with_offers.alias('cwo')
        
        j=outerjoin(contacts_with_offers,ВхТабл.customer_reactions,
                                and_(cast(contacts_with_offers.c.offer_id,DECIMAL(18,0))==
                                     cast(ВхТабл.customer_reactions.c.offer_id,DECIMAL(18,0)),
                                     cast(contacts_with_offers.c.customer_id,DECIMAL(18,0))==
                                     cast(ВхТабл.customer_reactions.c.gold_customer_id,DECIMAL(18,0))
                                    )
                    )
                    
        contacts_with_offers_and_answers=select(contacts_with_offers.c+[cast(getattr(ВхТабл.customer_reactions.c,i),INT).label(i)
                                                    if i!='operator_office' else getattr(ВхТабл.customer_reactions.c,i)
                                                    for i in cols_from_customer_reactions]).select_from(j).\
                                                    alias('c1')
        
        self.Таблица_запросов[1]=(contacts_with_offers_and_answers,'Таблица с подтянутыми ответами клиентов из customer_reactions и офферами')
        
        
    def addCdmMaOffer(self,contacts_with_offers_and_answers,ВхТабл):
        sel_offers_id=select([cast(contacts_with_offers_and_answers.c.offer_id,DECIMAL)]).\
                                    where(contacts_with_offers_and_answers.c.offer_id!=None)
        cdm_ma_offer=select([getattr(ВхТабл.inp_cdm_ma_offer.c,i) for i in ['participant_id','product_cd','offer_id']]).\
                                        where(ВхТабл.inp_cdm_ma_offer.c.offer_id.in_(sel_offers_id)).alias('cdm_ma_offer')
                                        
        j=outerjoin(contacts_with_offers_and_answers,cdm_ma_offer,
                                cast(contacts_with_offers_and_answers.c.offer_id,DECIMAL(18,0))\
                                ==cast(cdm_ma_offer.c.offer_id,DECIMAL(18,0)))
                                
        j2=j.outerjoin(ВхТабл.ma_product_dict,j.c.cdm_ma_offer_product_cd==ВхТабл.ma_product_dict.c.product_cd)
        
        self.Таблица_запросов[2]=(select(contacts_with_offers_and_answers.c+\
        [j2.c.cdm_ma_offer_product_cd,j2.c.etl_data_mart_cdm_ma_product_dict_product_name]).\
                    select_from(j2).alias('from_cdm'),'Таблица c подтянутыми cdm_ma_offer')
                    
    
    def addCampaignNames(self,with_products,ВхТабл):
        unique_cdm_ma_campaign=select([ВхТабл.cdm_ma_campaign.c.campaign_cd,ВхТабл.cdm_ma_campaign.c.campaign_nm]).\
                select_from(ВхТабл.cdm_ma_campaign).distinct().alias('un')
                
        j=outerjoin(with_products,unique_cdm_ma_campaign,
                                with_products.c.campaign_cd==unique_cdm_ma_campaign.c.campaign_cd)
        
        self.Таблица_запросов[3]=(select([getattr(with_products.c,j) for j in [i.name for i in with_products.c]]+\
                                    [j.c.un_campaign_nm]).select_from(j).alias('from_campaigns'),
                                    'Подтягивание campaign_name')
                                    

    def addTelecardContacts(self,ВхТабл):
        lk_MOBAPP_OFFER_HISTORY=Table('cdm_mobapp_offer_history'.lower(),
                                                Base.metadata,schema='etl_data_mart',autoload=True,autoload_with=engine)
        
        s1=select([cast(lk_MOBAPP_OFFER_HISTORY.c.gold_customer_id,DECIMAL(18,0)).label('gold_customer_id'),
        cast(lk_MOBAPP_OFFER_HISTORY.c.creation_dt,TIMESTAMP).label('creation_dt'),
        cast(lk_MOBAPP_OFFER_HISTORY.c.offer_id,BIGINT).label('offer_id'),
        lk_MOBAPP_OFFER_HISTORY.c.product_cd,
        lk_MOBAPP_OFFER_HISTORY.c.campaign_cd])
        s1=s1.distinct().select_from(lk_MOBAPP_OFFER_HISTORY)
        s1=s1.where(and_(lk_MOBAPP_OFFER_HISTORY.c.product_cd!='Rejected',
                         lk_MOBAPP_OFFER_HISTORY.c.product_cd!='Convert')).alias('t1')
        
        j=outerjoin(s1,ВхТабл.main_client_register,ВхТабл.main_client_register.c.gold_customer_id==s1.c.gold_customer_id).\
        outerjoin(ВхТабл.branch_dict,ВхТабл.main_client_register.c.branch_entity_id==ВхТабл.branch_dict.c.branch_id)
        mob_app=select([text('"mob_app" AS channel_cd'),
                        text('"SENT" AS contact_history_status_cd'),
                        text('10000000000+ROW_NUMBER() OVER(ORDER BY t1.gold_customer_id,creation_dt) AS contact_id'),
                        text('0 AS ctrl_grp_flg'),
                        cast(s1.c.gold_customer_id,INT).label('customer_id'),
                        text('"SENT" AS delivery_status'),
                        text('NULL AS participant_id'),
                        s1.c.creation_dt.label('contact_dttm'),
                        ВхТабл.branch_dict.c.branch_id,
                        ВхТабл.branch_dict.c.branch_nm,
                        cast(s1.c.offer_id,INT).label('offer_id'),
                        s1.c.campaign_cd,
                        text('NULL AS agree_resp'),
                        text('NULL AS think_resp'),
                        text('NULL AS delay_resp'),
                        text('NULL AS sounded_resp'),
                        text('NULL AS decline_resp'),
                        text('NULL AS operator_office'),
                        s1.c.product_cd,
                        s1.c.product_cd.label('product_name'),
                        s1.c.product_cd.label('campaign_nm'),
                        text('"No product" AS crm_prod_nm')
                        ]).select_from(j)
        self.telecard_query="""INSERT INTO sbx_005.lk_del3 """+cmp(mob_app,ress=True)
        
        
    def _determine_product(self,x:pd.DataFrame):
        campaign_nm=x.campaign_nm
        product_cd=x.product_cd
        #"""Определить продукт по названию кампании"""
        if isinstance(campaign_name,str):
            if ' DOLPHIN '.lower() in campaign_name.lower()\
            or ' PS '.lower() in campaign_name.lower()\
            or ' dolphin+PS '.lower() in campaign_name.lower():
                return 'dolphin'
            elif ' casatka '.lower() in campaign_name.lower():
                return 'casatka'
            elif 'pills'.lower() in campaign_name.lower() or 'ln_' in campaign_name.lower() \
                or 'top pills' in campaign_name.lower():
                return 'pills'
            elif 'gepard' in campaign_name.lower() and 'gepard_save' not in campaign_name.lower() and 'gepard_saved' not in product_cd:
                return 'gepard'
            elif 'gepard_save' in campaign_name.lower() or 'gepard_save' in product_cd:
                return 'gepard_saved'
            elif 'monkey'.lower() in campaign_name.lower():
                return 'monkey'
            elif 'alert'.lower() in campaign_name.lower():
                return 'No product'
            elif any([i in campaign_name for i in ['Giraffik','Rhinocerus','Lion','Cobra']]):
                return 'No product'
                
        else:
            print('Неизвестная кампания!!--'+campaign_name)
            raise AssertionError
            
    
    
    
    
    def addProdColumn(self,inpTable):
        #добавляет CASE определение продукта к запросу/таблице inpTable
        заглушка=True
        if not заглушка:
            if inpTable is None:
                inpTable=self.final_offer_table
            campaigns=e(cmp(select([inpTable.c.campaign_nm,inpTable.c.product_cd]).distinct().select_from(inpTable),
                        ress=True))
            campaigns.loc[:,'crm_prod_nm']=campaigns.apply(self._determine_product,axis=1)
            load_to_hadoop('sbx_005.lk_camp_dict',campaigns)
            print('lk_camp_dict','Загружено')
        lk_camp_dict=assign_tab('lk_camp_dict',schema='sbx_005')
        
        subquery=select([getattr(lk_camp_dict.c,i) for i in ['campaign_nm','product_cd','crm_prod_nm']]).\
                distinct().alias('t1')
             
        j=inpTable.outerjoin(subquery,
                            and_(inpTable.c.campaign_nm==subquery.c.campaign_nm,
                                 inpTable.c.product_cd==subquery.c.product_cd)
             )
        res=select([getattr(inpTable.c,i.name) for i in list(inpTable.c)]+[subquery.c.crm_prod_nm]).select_from(j)
        self.Таблица_запросов[4]=(res,'Добавление crm_prod_nm')
        
        
############################################################################################################
#Основной класс для расчета таблиц по откликам в sbx_005
############################################################################################################

class MY_EXAMPLE_report():
    def __init__(self,start_with=0):
        self.table_count=0
        self.список_таблиц=[]
        self.Таблица_запросов={}
        self.mode='prod'
        self.start_with=start_with
        self.report_data={}
        
    def script(self,mob_app_add=1):
        self.Comms=Comms()
        
        self.Comms.addBranchId(Входные_таблицы)
        self.Comms.s1=create_table_from_query(self,self.Comms.Таблица_запросов[0][0],table_name=None)
        print('-'*100)
        self.Comms.addOffersAndAnswers(self.Comms.s1,Входные_таблицы)
        self.Comms.contacts_with_offers_and_answers=create_table_from_query(self,self.Comms.Таблица_запросов[1][0],table_name=None)
        
        self.Comms.addCdmMaOffer(self.Comms.contacts_with_offers_and_answers,Входные_таблицы)
        self.Comms.with_products=create_table_from_query(self,self.Comms.Таблица_запросов[2][0],table_name=None)
        
        self.Comms.addCampaignNames(self.Comms.with_products,Входные_таблицы)
        self.Comms.addProdColumn(self.Comms.Таблица_запросов[3][0])
        
        self.Comms.final_offer_table=create_table_from_query(self,self.Comms.Таблица_запросов[4][0])
        
        if self.table_count>=self.start_with and mob_app_add==1:
            self.Comms.addTelecardContacts(Входные_таблицы)
            e(self.Comms.telecard_query)
            
        self.communications=self.Comms.final_offer_table
        
        self._get_list_of_resp_queries()
        
        return None
        
        
    def _create_query_for_adding_resps(self,prod_name):
        resp_cnt=ResponseCounter(prod_name,self.Comms)
        self.Таблица_запросов['отклики_'+prod_name]=resp_cnt.subtable_with_responses_and_batches
        return self.Таблица_запросов['отклики_'+prod_name]
        
    def _get_list_of_resp_queries(self):
        #получить имена кампаний по каждой продукту
        self.prods=self.Comms.список_продуктов
        queries={}
        for crm_prod_nm in self.prods:
            if crm_prod_nm!='No product':
                queries[crm_prod_nm]=self._create_query_for_adding_resps(crm_prod_nm)
        self.crm_prod_req_queries=queries
        
    def создать_таблицу_откликов(self):
        table_with_resps='sbx_005.lk_del'+str(self.table_count)
        table_with_resps_name=table_with_resps
        
        
        self.table_with_resps_name=table_with_resps_name
        if self.mode=='prod' and self.table_count>=self.start_with:
            e(f"""DROP TABLE IF EXISTS {table_with_resps}""")
            
        #для каждого продукта получить список откликов и вставить его в таблицу
        for crm_prod_nm,query_text in self.crm_prod_req_queries.items():
            if table_with_resps.split('.')[-1] not in e("""SHOW TABLES IN sbx_005""").name.tolist():
                create_table_from_query(self,text(query_text),
                                            table_name=table_with_resps)
            else:
                if self.table_count>=self.start_with:
                    e(f"""INSERT INTO {table_with_resps}
                            {query_text}""")
            
    def Добавить_число_откликов_к_коммуникациям(self):
        print(self.table_count)
        if self.table_count>=self.table_start_with:
            result_table='sbx_005.lk_del'+str(self.table_count)
            e(f"""DROP TABLE IF EXISTS {result_table}""")
            e(f"""CREATE TABLE {result_table} AS 
                (
                SELECT {','.join(['t1.'+i.name for i in list(self.communications.c)])},
                        t2.request_id,t2.issue_id,t2.request_dt,t2.issue_dt,t2.issue_sum,t2.cnt_batch_contacts_after
                        
                FROM sbx_005.{self.communications.name} t1
                LEFT JOIN (SELECT cast(customer_id AS DECIMAL(18,0)) AS customer_id,
                        CAST(offer_id AS DECIMAL(18,0)) AS offer_id,
                        contact_id,
                        COUNT(request_id) AS request_id,
                        COUNT(issue_id) AS issue_id,
                        MIN(request_dt) AS request_dt,
                        MIN(issue_dt) AS issue_dt,
                        SUM(issue_sum) AS issue_sum,
                        SUM(cnt_batch_contacts_after) AS cnt_batch_contacts_after
                    FROM {self.table_with_resps_name}
                    GROUP BY customer_id,offer_id,contact_id
                    ) t2 ON t1.customer_id=t2.customer_id AND CAST(t1.offer_id AS DECIMAL(18,0))=t2.offer_id
                    AND t1.contact_id=t2.contact_id
                )
            """)
            invalidate(result_table)
        self.final_table=Table(result_table.split('.')[-1],Base.metadata,
            schema='sbx_005',autoload=True,autoload_with=engine)
    
    
    
    
class ResponseCounter():
    def __init__(self,prod_name,Comms):
        self.таблица_коммуникаций=Comms.final_offer_table.name
        
        self.prod_name=prod_name
        self.запрос_к_таблице_заявок=Входные_таблицы.таблицы_CRM._get_query_for_response_table(prod_name)
        self.таблица_батчей=Входные_таблицы.таблицы_CRM.prods_batch_tables[prod_name]
        
        self.subtable=f"""SELECT channel_cd,campaign_nm,CAST(customer_id AS DECIMAL(18,0)) AS customer_id,
                        CAST(offer_id AS DECIMAL(18,0) AS offer_id,
                        CAST(contact_id AS DECIMAL(18,0)) AS contact_id,
                        CAST(contact_dttm AS TIMESTAMP) AS contact_dttm,
                        FROM sbx_005.{self.таблица_коммуникаций}
                        WHERE crm_prod_nm={"'"+self.prod_name+"'"}"""
        
        self.only_responses=f"""SELECT t1.*,t2.*
                FROM (
                        {self.subtable}
                    ) t1
                INNER JOIN (
                    {self.запрос_к_таблице_заявок[0]}
                ) t2 ON (t1.CUSTOMER_ID=t2.client_id_in_response_table)
                        AND (
                        
                        (t1.channel_cd='telegramm' AND (CAST(t2.request_dt AS TIMESTAMP)>=
                                TRUNC(CAST(t1.contact_dttm AS TIMESTAMP,'dd'))
                                and CAST(t2.request_dt AS TIMESTAMP)< date_add(TRUNC(CAST(t1.contact_dttm AS TIMESTAMP),'dd'),1))
                        )
                        OR 
                        (t1.channel_cd IN ('telepathy','mob_app') AND (CAST(t2.request_dt AS TIMESTAMP)>=
                                TRUNC(CAST(t1.contact_dttm AS TIMESTAMP,'dd'))
                                and CAST(t2.request_dt AS TIMESTAMP)< date_add(TRUNC(CAST(t1.contact_dttm AS TIMESTAMP),'dd'),7))
                        
                        
                        )
                        )
                        """
        self.only_last_communication=f"""
                SELECT * 
                FROM 
                (
                SELECT t1.*,ROW_NUMBER() OVER(PARTITION BY request_id ORDER BY contact_dttm DESC) AS rn_
                FROM (
                        {self.only_responses}
                    )
                ) last_comms
                WHRE last_comms.rn_=1
                """
        self.subtable_with_responses=f"""
        SELECT t1.channel_cd,t1.campaign_nm,
        t1.customer_id,t1.contact_id,t1.offer_id,t1.contact_dttm,
        t1.client_id_in_response_table,t2.product_nm,
        CAST(t2.request_id AS DECIMAL(18,0)) AS request_id,
        CAST(t2.request_dt AS TIMESTAMP) AS request_dt,t2.issue_id,
        CAST(t2.issue_dt AS TIMESTAMP) AS issue_dt,
        CAST(t2.start_dt AS TIMESTAMP) AS start_dt,
        t2.issue_sum
        FROM (
            {self.subtable}
            ) t1
        LEFT JOIN (
                {self.only_last_communication}
        ) t2 ON (t1.contact_id=t2.contact_id AND t1.offer_id=t2.offer_id)
        OR (t1.customer_id=t2.customer_id AND t1.contact_id=t2.contact_id AND t1.channel_cd='mob_app')
        """
        
        self.subtable_with_responses_and_batches=f"""
        SELECT t1.channel_cd,t1.campaign_nm,t1.customer_id,t1.contact_id,t1.offer_id,t1.contact_dttm,
        t1.client_id_in_response_table,t1.product_nm,t1.request_id,t1.request_dt,t1.issue_id,t1.issue_dt,t1.start_dt,
        CAST(t1.issue_sum AS DECIMAL(25,2)) AS issue_sum,
        COUNT(t2.contact_dt) AS cnt_batch_contacts_after
        FROM (
            {self.subtable_with_responses}
        
            ) t1
            
        LEFT JOIN (
            SELECT customer_id,contact_dt
            FROM sbx_005.{таблица_батчей}
                ) t2
                ON t1.customer_id=t2.customer_id AND CAST(t1.contact_dttm AS TIMESTAMP)<CAST(t2.contact_dt AS TIMESTAMP) AND    
                            (CAST(t2.contact_dt AS TIMESTAMP)<CAST(t1.request_dt AS TIMESTAMP) OR t1.request_dt IS NULL)
        GROUP BY t1.channel_cd,t1.campaign_nm,t1.customer_id,t1.contact_id,t1.offer_id,t1.contact_dttm,
                t1.client_id_in_response_table,t1.product_nm,t1.request_id,t1.request_dt,t1.issue_id,t1.issue_dt,t1.start_dt,t1.issue_sum
        
        """
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
        
        
        
             
             
             
             
             
             
            
            
            
            
                        
    
                    
        
                                                    
        
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
                                                    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        