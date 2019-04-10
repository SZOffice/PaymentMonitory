# -*- coding:utf-8 -*-

import pymssql
import time
import datetime
import os
from slackclient import SlackClient

##connection to slack channel
slack_token = "***********************************"
sc = SlackClient(slack_token)

class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise NameError("没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise NameError("连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()



end_time_format = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
end_time = datetime.datetime.now().strftime("%H:%M:%S")
start_time_format = (datetime.datetime.now()+datetime.timedelta(hours=-2)).strftime("%Y-%m-%d %H:%M:%S")
start_time = (datetime.datetime.now()+datetime.timedelta(hours=-2)).strftime("%H:%M:%S")



def Checking_iPay88_Data(target_country):
    if target_country=='HK':
        ms_Conn = MSSQL(host="HKPDRSQL1",user="***********",pwd="*********",db="JobsDBHK")
        sql = "SELECT PurchaseOrderID,AccountNum,SubAccount,PurchaseDate,PackageAmount,PaymentTermsID,PaymentTermsDesc,ResponseType,ResponsedBy FROM Billing_PurchaseOrder_Master WHERE PurchaseDate BETWEEN '"+start_time_format+"' and '"+end_time_format+"' and ResponsedBy =1 and PaymentTermsID=11 and ResponseType='P' order by PurchaseDate asc"
    elif target_country=='TH':
        ms_Conn = MSSQL(host="THPDRSQL1",user="********",pwd="*********",db="JobsDBTH")
        sql = "SELECT PurchaseOrderID, AccountNum,SubAccount,PurchaseDate,PackageAmount,PaymentTermsID,PaymentTermsDesc,ResponseType,ResponsedBy FROM Billing_PurchaseOrder_Master WHERE PurchaseDate BETWEEN '"+start_time_format+"' and '"+end_time_format+"' and ResponsedBy =1 and PaymentTermsID=13 and ResponseType='P' order by PurchaseDate asc;"
    elif target_country=='ID':
        ms_Conn = MSSQL(host="IDPreSQL1",user="*********",pwd="**********",db="PDS_JobsDBID")
        sql = "SELECT AccountNum,SubAccount,PurchaseDate,TotalAmount,PaymentTermsID,PaymentTermsDesc,ResponseType,ResponsedBy FROM Billing_PurchaseOrder_Master WHERE PurchaseDate BETWEEN '"+start_time_format+"' and '"+end_time_format+"' and ResponsedBy =1 and TotalAmount>0 and ResponseType='P' order by PurchaseDate asc ;"
    
    print("--Start scanning online payment "+target_country+" ---")
    
    reslist = ms_Conn.ExecQuery(sql)
    outputInfo =''
    if  len(reslist) == 0:
        list_all = None
        print("Great, everything is going well")
    elif len(reslist) >= 5:
        list_all = []
        time =0
        for i in reslist:
            time=time+ 1
            list_all.append(i[0])
        print(list_all)
        outputInfo = "(DBRC %s Online payment)during from %s to %s total failed %s times, Order ref No.: %s " %(target_country,start_time,end_time,time,list_all)
    else:
        list_all = None
        print("Less than 5 records failed") 
    
    print("--End scan "+target_country+" ---")
    return outputInfo




#call API send message to channel 
def slack_send(msg):  
    sc.api_call(
        "chat.postMessage",
        channel="********",
        text=msg
    )
country_list = ["HK","ID","TH"]    
for target_country in country_list:
    Sendmsg = Checking_iPay88_Data(target_country)
    if Sendmsg != None:
            slack_send(Sendmsg)
