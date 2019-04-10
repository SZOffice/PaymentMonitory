# -*- coding: utf-8 -*-  
import pymysql
import time
import datetime
import os
from slackclient import SlackClient

##connection to slack channel
slack_token = "*****************************"
sc = SlackClient(slack_token)







def Checking_iPay88_Data(target_country):


     # SQL 查询语句
    
    end_time_format = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.now().strftime("%H:%M:%S")
    start_time_format = (datetime.datetime.now()+datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H:%M:%S")
    start_time = (datetime.datetime.now()+datetime.timedelta(hours=-1)).strftime("%H:%M:%S")

    sql = """SELECT r.id as transactionid  FROM receipts r 
    inner join temp_sales_orders tso on r.id=tso.receipt_id
    LEFT JOIN `receipt_jobs` ON (`receipt_jobs`.`receipt_id` = r.`id`) 
    LEFT JOIN `jobs` ON ( `jobs`.`id` = `receipt_jobs`.job_id ) LEFT join ref_payment_statuses rps on rps.`code`=r.payment_status_code
    WHERE r.payment_method_code=1 and r.payment_status_code= 2
        AND (tso.job_flag = 0 OR ( tso.job_flag = 1 AND `jobs`.`id` IS NOT NULL ) )
        AND tso.created_datetime >= '"""+start_time_format+"""' 
        AND tso.created_datetime <= '"""+end_time_format+"""' 
    GROUP BY
    r.id """

    try:
        # 打开数据库连接 "MY","SG","PH","ID","VN"]
        if target_country == "MY":
                db = pymysql.connect(
                        host='*********',
                        port=3306,
                        user='*********',
                        password='**********',
                        db='jbos',
                        charset='utf8'
                )
        elif target_country == "SG":
                db = pymysql.connect(
                        host='*********',
                        port=3308,
                        user='********',
                        password='***********',
                        db='jbos',
                        charset='utf8'
                )
        elif target_country == "*********":
                db = pymysql.connect(
                        host='************',
                        port=3306,
                        user='********',
                        password='************',
                        db='jbos',
                        charset='utf8'
                )
        elif target_country == "ID":
                db = pymysql.connect(
                        host='*********',
                        port=3306,
                        user='jbos_app',
                        password='***********',
                        db='jbos',
                        charset='utf8'
                )
        elif target_country == "VN":
                db = pymysql.connect(
                        host='*********',
                        port=3306,
                        user='jbos_app',
                        password='**********',
                        db='jbos',
                        charset='utf8'
                )
                

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        # 执行SQL语句
        cursor.execute(sql)
        print("Start scanning Ipay88 payment status between "+ start_time_format + " and " + end_time_format+ " Country:"+ target_country )
        # results是个元组对象
        results = cursor.fetchall()
        if len(results) == 0 :
            print("Great, everything is well")
        elif len(results) > 2:
            list_all = []
            TransactionID = 0
            times = 0
            Series_times = 0
            for row in results:
                times =times + 1
                if int(row[0])== TransactionID + 1:
                   list_all.append(row[0])
                   Series_times = Series_times + 1
                
                else:
                    list_all.append(row[0])
                    
                TransactionID = row[0]
            PrintInfo = " %s transations failed，number of iteration failures %s ，transationID= %s" %(times,Series_times,list_all)
            print(PrintInfo)

            print("Scanning End")
            return  "iPay88 monitor: Country "+target_country +" ( "+start_time+ " - "+end_time+" ) have "+PrintInfo+"，请及时在JBOS后台查看哦~  http://jbos-"+target_country+".jobstreet.com/Online_Payment_Recovery/listing"
        elif len(results) <= 2:
           print("failed transaction less than 2 rows")
            
    except Exception as e:
        print("Error: unable to fetch data")
        print(e)
    # 关闭游标
    finally:
        cursor.close()
    # 关闭数据库连接
        db.close()


#call API send message to channel 
def slack_send(msg):  
    sc.api_call(
        "chat.postMessage",
        channel="************",
        text=msg
    )
country_list = ["MY","SG","PH","ID","VN"]    
for target_country in country_list:
    Sendmsg = Checking_iPay88_Data(target_country)
    if Sendmsg != None:
            slack_send(Sendmsg)

