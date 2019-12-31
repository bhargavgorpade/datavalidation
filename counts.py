import datetime
import logging
import pyodbc
import time
import sys

###source - SQL Server Database Configuration Details
src_server = 'VRSQLREPORTS\REPORTS_PROD' 
src_database = 'stg_lsams' 
src_username = 'X' 
src_password = 'Y'

 ##Target - SQL Server Database Configuration Details
trgt_server = 'lsd-cus-uat-asq-001.database.windows.net' 
trgt_database = 'stg_lsams' 
trgt_username = 'X' 
trgt_password = 'Y'

###prod - SQL Server Database Configuration Details
prod_server = 'CRNTZSQLPRD01' 
prod_database = 'stg_lsams' 
prod_username = 'X' 
prod_password = 'Y'

##Connection strings
src_conn_str  = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+src_server+';Trusted_Connection=yes;'
trgt_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+trgt_server+';DATABASE='+trgt_database+';Trusted_Connection=yes;'
prod_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+prod_server+';DATABASE='+prod_database+';Trusted_Connection=yes;'

from multiprocessing import Pool

#import ray

global logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

def take_count(connectionDetails):
    #print("Details inside function")
    #print(connectionDetails)
    conn_type=connectionDetails['conn_type']
    conn_str=connectionDetails['conn_str']
    tableName=connectionDetails['tableName']
    #print(conn_type+conn_str+tableName)
    cnxn_trgt = pyodbc.connect(conn_str)
    crsr_trgt = cnxn_trgt.cursor()
    if(conn_type=='ISERV'):
        print("DB Iserv:"+conn_type)
        qry_trgt  ="SELECT * FROM OPENQUERY(AS400_LSAMS,'"+"select COUNT_BIG(1) from "+tableName+" with UR')"
    else:
        print("DB :"+conn_type)
        qry_trgt  ="select COUNT_BIG(1) from STG_LSAMS."+tableName+" WITH (NOLOCK)"
    query_start_now = datetime.datetime.now()
    crsr_trgt.execute(qry_trgt)
    res_trgt=crsr_trgt.fetchone()
    query_end_now = datetime.datetime.now()
    query_time_elapsed=query_end_now-query_start_now
    cnxn_trgt.close()
    #print("Returning"+str(res_trgt[0]))
    res={}
    res[conn_type]={'count':str(res_trgt[0]),'elapsedTime':str(query_time_elapsed)}
    return res
          
if __name__ == '__main__':     
    logger.info("Starting the program")
    tableNameList=sys.argv[1]
    Numberoftimes=int(sys.argv[2])
    waitTime=int(sys.argv[3])
    day=int(sys.argv[4])
    i=0
    while i<Numberoftimes:
        dttm_string_now = datetime.datetime.now()
        dttm_string = dttm_string_now.strftime("%d_%b_%Y_%H_%M_%S")
        issue_file_name="C:/Demo/Validation_Tool/Output/"+"counts_"+tableNameList+"_"+dttm_string+".csv";
        out_file_issues=open(issue_file_name,"w")
        out_file_issues.write("Day,Table,CRNTZ_COUNT,HYPER_COUNT,STARTTIME,ISERV_COUNT,ELAPSED_TIME_ISERV,ELAPSED_TIME_HYPER,ISERV_CRNTZ_DELTA,HYPER_CRNTZ_DELTA,ISERV_HYPER_DELTA,StartDateTime,ELAPSED_TIME_CRNTZ")        
        in_cntrl=open("C:/Demo/Validation_Tool/Input/"+tableNameList,"r").readlines()[1:]
        res={}
        cus_list={}
        for rec in in_cntrl:
            rec_list=rec.replace("\n","").split(",")
            cus_list[rec_list[0]]=rec_list
        for table_name in cus_list.keys():
                custom_col=cus_list[table_name]
                Report_start_now = datetime.datetime.now()
                #logger.info("Starting Time: "+str(Report_start_now)) 
                #logger.info("***Count Query for *** "+table_name+" ***")
                try:
                    p = Pool(processes=3)
                    print("Process Created for table "+custom_col[0])
                    conn_list={}
                    data={}
                    conn_list['ISERV']={'conn_type':'ISERV','conn_str':src_conn_str,'tableName':custom_col[1]}
                    conn_list['HYPER']={"conn_type":'HYPER','conn_str':trgt_conn_str,'tableName':custom_col[2]}
                    conn_list['CRNTZ']={"conn_type":'CRNTZ','conn_str':prod_conn_str,'tableName':custom_col[3]}
                    
                    #print(conn_list)
                    data = p.map(take_count,[conn_list[conn] for conn in conn_list])
                    p.close()
                    print(data)
                    src_countid=data[0]['ISERV']
    
                    trgt_countid=data[1]['HYPER']
    
                    prod_countid=data[2]['CRNTZ']
                    Report_end_now = datetime.datetime.now()
                    #logger.info("Ending Time: "+str(Report_end_now))
                    Report_time_elapsed=Report_end_now-Report_start_now
                    print("Time elapsed : " + str(Report_time_elapsed))
                    #Day,Table,CRNTZ_COUNT,HYPER_COUNT,STARTTIME,ISERV_COUNT,ELAPSED_TIME_ISERV,ELAPSED_TIME_HYPER,ISERV_CRNTZ_DELTA,HYPER_CRNTZ_DELTA,ISERV_HYPER_DELTA,StartDateTime,ELAPSED_TIME_CRNTZ
                    output="\n"+str(day)+","+table_name+","+prod_countid['count']+","+trgt_countid['count']+","+str(Report_start_now)+","+src_countid['count']+","+src_countid['elapsedTime']+","+trgt_countid['elapsedTime']+","+str(int(src_countid['count'])-int(prod_countid['count']))+","+str(int(trgt_countid['count'])-int(prod_countid['count']))+","+str(int(src_countid['count'])-int(trgt_countid['count']))+","+str(Report_start_now.strftime("%d-%b-%y"))+","+prod_countid['elapsedTime']
                    print(output)
                    out_file_issues.write(output)
                except Exception as e:
                    print("exception is"+str(e))                                           
        i=i+1
        print("Wait for reqd Seconds..")
        time.sleep(waitTime)
        out_file_issues.close()
    logger.info("Ending the program")