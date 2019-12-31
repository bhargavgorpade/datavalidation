import datetime
import logging
#
from rdbms_unloader import rdbms_unloader,collection_exist,get_sql
from metadata_processer import get_metadata_from_table,metadata_comparsion
from data_compare import data_comparsion
from config_code import get_orderby,embedded_collection_list,get_source_connection,get_target_connection,set_row_count,empty_row_count,get_source_db_type,get_target_db_type
from report_generation import html_report_generation
#
global trgt_conn_str,src_conn_str,cus_list,keys,orderbykeys
global logger
#
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
cus_list = embedded_collection_list()
orderbykeys = get_orderby()
trgt_conn_str = get_target_connection()
src_conn_str = get_source_connection()
report_res = {}
#
def validation_job(table_name):
#
    logFile = "C:/Demo/Validation_Tool/Log/"+ table_name +'_debug.log'
    fh = logging.FileHandler(logFile)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("Starting the validation for "+ table_name)
    empty_row_count()
    custom_col = cus_list[table_name]
#
    if collection_exist(trgt_conn_str,table_name) == 1:
        logger.debug("Processing of "+ table_name +" in Source DB has started")
        orderbylist = orderbykeys[table_name]
        orderby = ','.join(str(orderbytoken) for orderbytoken in orderbylist)
        logger.debug("The KEY corresponding to "+ table_name +" is "+ orderby)
        alias = custom_col[8]
        wherecondition = custom_col[9]
        logger.debug("The ALIAS corresponding to "+ table_name +" is "+ alias)
        logger.debug("The WHERE condition corresponding to "+ table_name +" is "+ wherecondition)
#
        DBType = get_source_db_type()
#
        logger.debug("The DBTYPE identified for Source is "+ DBType)
#
        formatted_src_data,src_row_count,full_src_count = rdbms_unloader(table_name,orderbykeys,get_sql(src_conn_str,custom_col[6],DBType,custom_col[2],orderby,alias,wherecondition,custom_col[3],custom_col[12],custom_col[14]),'Source',custom_col[4],custom_col[13])
#
        set_row_count(full_src_count)
#
        logger.debug("Total no of records unloaded: "+ str(len(formatted_src_data)))
        logger.debug("Processing of "+ table_name +" in Target DB has started")
        alias = custom_col[7]
        wherecondition = custom_col[10]
        logger.debug("The ALIAS corresponding to "+ table_name +" is "+ alias)
        logger.debug("The WHERE condition corresponding to "+ table_name +" is "+ wherecondition)
#  
        DBType = get_target_db_type()
#
        logger.debug("The DBTYPE identified for Target is "+ DBType)
#
        formatted_trg_data,trgt_row_count,full_trgt_count = rdbms_unloader(table_name,orderbykeys,get_sql(trgt_conn_str,custom_col[5],DBType,custom_col[0],orderby,alias,wherecondition,custom_col[3],custom_col[12],custom_col[14]),'Target',custom_col[4],custom_col[13])
#
        set_row_count(full_trgt_count)
#
        logger.debug("Total no of records unloaded: "+ str(len(formatted_trg_data)))
        logger.debug("Metadata Extraction for "+ table_name +" in Source DB - Started")
#       
        src_meta = get_metadata_from_table(src_conn_str,table_name,custom_col[12])
#        
        logger.debug("Metadata Extraction for "+ table_name +" in Target DB - Started")
#       
        trg_meta = get_metadata_from_table(trgt_conn_str,table_name,custom_col[12])
#
        logger.debug("Metadata Comparison for "+ table_name +" in Source and Target DBs - Started")
#
        meta_res = metadata_comparsion(src_meta,trg_meta)
#
        logger.debug("Metadata Comparison for "+ table_name +" in Source and Target DBs - Ended")
        logger.debug("Data Comparison for "+ table_name +" in Source and Target DBs - Started")
#       
        data_comp_res = data_comparsion(src_meta,trg_meta,formatted_src_data,formatted_trg_data,'Y')
#
        logger.debug("Data Comparison for "+ table_name +" in Source and Target DBs - Ended")
#
        formatted_src_data = None
        formatted_trg_data = None
        data_comp_res[0].append(src_row_count)
        data_comp_res[0].append(trgt_row_count)
        data_comp_res.append(meta_res)
        return data_comp_res
    else:
        return ([])
#
if __name__ == '__main__':     
    logger.info("Starting the program")
    report_res = {}
    dttm_string_now = datetime.datetime.now()
    dttm_string = dttm_string_now.strftime("%d_%b_%Y_%H_%M_%S")
    issue_file_name = "C:/Demo/Validation_Tool/Output/issues.csv"
    out_file_issues = open(issue_file_name,"w")
    out_file_issues.write("TABLE NAME,TARGET FIELD,SOURCE FIELD,FIELD EXIST IN TARGET,FIELD EXIST IN SOURCE,TARGET DATATYPE,SOURCE DATATYPE,TARGET DATALENGTH,SOURCE DATALENGTH,FIELD VALIDATION STATUS,MISMATCH COUNT,TARGET MISMATCH SAMPLE,SOURCE MISMATCH SAMPLE,KEY VALUES,KEY COLUMN LIST")
    report_file_name = "C:/Demo/Validation_Tool/Output/report.csv"
    out_file_report = open(report_file_name,"w")
    out_file_report.write("SOURCE ENTITY,TARGET ENTITY,SOURCE RECORD COUNT,TARGET RECORD COUNT,SOURCE RECORD COUNT (DISTINCT),TARGET RECORD COUNT (DISTINCT),MATCHED RECORD COUNT,SOURCE DIFFERENCE COUNT,TARGET DIFFERENCE COUNT,METADATA CHECK,DATA CHECK,OVERALL STATUS,KEY COLUMN LIST")
    out_file_issues.close()
    out_file_report.close()
    for table_name in cus_list.keys():
        try:
            custom_col = cus_list[table_name]
#
            if(custom_col[11] == 'Y'):
                val_start_now = datetime.datetime.now()
                logger.info("Validation Starting Time: "+ str(val_start_now))
#
                report_res[table_name] = validation_job(table_name)
#
                val_end_now = datetime.datetime.now()
                logger.info("Validation Ending Time : "+ str(val_end_now))
                val_time_elapsed=val_end_now-val_start_now
                logger.info("Validation Elapsed Time: "+ str(val_time_elapsed))
                logger.info("Report Generation Started for "+ table_name) 
                Report_start_now = datetime.datetime.now()
                logger.info("Report Generation Starting Time: "+ str(Report_start_now))
                tablekey = ','.join(str(tablekeytoken) for tablekeytoken in orderbykeys[table_name])
#
                logger.debug(tablekey)
#               
                html_report_generation(report_res,table_name,tablekey,custom_col[13])
#
                logger.info("Report Generation Completed for "+ table_name)
                Report_end_now = datetime.datetime.now()
                logger.info("Report Generation Ending Time  : "+ str(Report_end_now))
                Report_time_elapsed=Report_end_now-Report_start_now
                logger.info("Report Generation Elapsed Time : " + str(Report_time_elapsed))
                report_res = {}
#
        except Exception as e:
            print("exception is"+ str(e))
            continue
#
    logger.info("Ending the program")