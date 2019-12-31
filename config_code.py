#import cx_Oracle
#Fill in proper details before execution of script
## PARALLEL TEST
src_server = 'SRC_SERVER' 
src_database = 'SRC_DB'
#src_username = 'ABCD'
#src_password = 'abcd'
 ##Target - SQL Server Database Configuration Details
trgt_server = 'TRGT_SERVER'
trgt_database = 'TRGT_DB'
#trgt_server = 'TRGT_SERVER'
#trgt_database = 'TRGT_DB'
 ##Connection strings
src_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+src_server+';DATABASE='+src_database+';Trusted_Connection=yes;'
trgt_conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+trgt_server+';DATABASE='+trgt_database+';Trusted_Connection=yes;'
#src_conn_str = 'DRIVER={NetezzaSQL};SERVER='+src_server+';DATABASE='+src_database+';UID='+src_username+';PWD='+ src_password
#dsn_tns = cx_Oracle.makedsn('host', 'port', service_name='service name')  
#src_conn_str = cx_Oracle.connect(user='username', password='password', dsn=dsn_tns) 
#
row_count=[]
#
def get_row_count():
    return row_count
#
def set_row_count(rc):
    row_count.append(rc)
#
def empty_row_count():
    row_count.clear()
#
def get_source_connection():
    return src_conn_str
#
def get_target_connection():
    return trgt_conn_str
#
def get_source_db():
    return 'SERVER='+src_server+' DATABASE='+src_database
#
def get_target_db():
    return 'SERVER='+trgt_server+' DATABASE='+trgt_database
#
def get_source_db_type():
    if src_conn_str.find('SQL Server') > 0:
        DBType = 'SQL'
    elif src_conn_str.find('Netezza') > 0:
        DBType = 'Netezza'
    else:
        DBType = 'Oracle'
    return DBType
#
def get_target_db_type():
    if trgt_conn_str.find('SQL Server') > 0:
        DBType = 'SQL'
    elif trgt_conn_str.find('Netezza') > 0:
        DBType = 'Netezza'
    else:
        DBType = 'Oracle'
    return DBType
#
def get_orderby():
    in_cntrl=open("C:/Demo/Validation_Tool/Input/table_key_config.csv","r").readlines()[1:]
    res={}
#
    for rec in in_cntrl:
        rec_list = rec.replace("\n","").split(",")
        res[rec_list[0]]=rec_list[1].split("|") 
#
    return res
#
def embedded_collection_list():
    in_cntrl=open("C:/Demo/Validation_Tool/Input/table_key_config.csv","r").readlines()[1:]
    res={}
#
    for rec in in_cntrl:
        rec_list = rec.replace("\n","").split(",")
        res[rec_list[0]]=rec_list
#
    return res
#
def get_source_table_name(collection_name):
    col = embedded_collection_list()
    source_table = col[collection_name][2].replace("|",",")
    return source_table
#
def get_column_name_query(conn_str,tab_schema,tab_name):
    if conn_str.find('SQL Server') > 0:
        query = "SELECT DISTINCT COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= '"+ tab_name +"' AND TABLE_SCHEMA = '"+ tab_schema +"'"
    return query
#
def get_metadata_query(conn_str,tab_schema,tab_name):
    if conn_str.find('SQL Server') > 0:
        query = "SELECT COLUMN_NAME,DATA_TYPE AS DATA_TYPE,ISNULL(CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR),CAST(NUMERIC_PRECISION AS VARCHAR)) AS SIZE_LENGTH,CAST(NUMERIC_SCALE AS VARCHAR) AS SIZE_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+ tab_schema +"' AND TABLE_NAME='"+ tab_name +"' ORDER BY TABLE_NAME,COLUMN_NAME"
    return query
