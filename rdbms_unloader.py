import pyodbc
import datetime
import csv
#
from config_code import get_column_name_query
#
def collection_exist(conn_str,table_name):
#
    if conn_str.find('Oracle') < 0:
        cnxn = pyodbc.connect(conn_str)
    else:
        cnxn = conn_str
#
    crsr = cnxn.cursor()
#
    if crsr.tables(table=table_name, tableType='TABLE').fetchone():
        res=1
    else:
        res=1
#
    cnxn.close()
    return res
#
def get_sql(conn_str,sql_file,DBType,table_name,orderbykeys,alias,wherecondition,limitcount,remove_column,ordering_type):
#    
    if conn_str.find('Oracle') < 0:
        cnxn = pyodbc.connect(conn_str)
    else:
        cnxn = conn_str
#
    crsr = cnxn.cursor()
    tab_name_list = table_name.split(".")
    tab_schema = tab_name_list[0]
    tab_name = tab_name_list[1]
    qry = get_column_name_query(conn_str,tab_schema,tab_name)
    crsr.execute(qry)
    result = list()
    rows = crsr.fetchall()
#
    for row in rows:
        result.append(row)
#
    flattened  = [val for sublist in rows for val in sublist]
#
    if (remove_column != '') and remove_column in  flattened:
        remove_column_list = str(remove_column).split('|')

        for rem_col_nm in remove_column_list:
            flattened.remove(rem_col_nm)
#
    col_string = '['+'],['.join(str(fields) for fields in flattened) + ']'
    limitcount = str(limitcount)
    queries = []
    orderbykeys = '['+'],['.join(str(keys) for keys in orderbykeys.split(',')) + ']'
    orderbykeys = orderbykeys.split(',')
#    
    if ordering_type == '':
        ordering_type = '|'.join('A' for num in range(len(orderbykeys)))
    elif len(ordering_type.replace('|','')) < len(orderbykeys):
        ordering_type = ordering_type.strip('|') + '|' + '|'.join('A' for num in range(len(orderbykeys) - len(ordering_type.replace('|',''))))
#
    ordering_type = ordering_type.replace('A','ASC').replace('D','DESC')
    ordering_type = ','.join(str(keys) for keys in ordering_type.split('|'))
    ordering_type = ordering_type.split(',')        
    ordering_key_type = ' ,'.join(str(ord_key)+' '+ ord_type for ord_key,ord_type in zip(orderbykeys,ordering_type))
#
    if (sql_file == ""):
#
        if(limitcount!=""):
           limitstr = "SELECT TOP " + limitcount
        else:
           limitstr = "SELECT "
#
        if (alias!=""):
            sql_content = " * FROM "+ table_name +" "+ alias +" WHERE "+ wherecondition +" ORDER BY "+ ordering_key_type
        else: 
            sql_content= " "+ col_string +" FROM "+ table_name +"  WHERE "+ wherecondition +" ORDER BY "+ ordering_key_type 
#
        sql_content_temp = sql_content[:sql_content.rfind(" ORDER BY ")]
        sql_count_content = "SELECT COUNT(*) from (SELECT "+ sql_content_temp +") AS COUNT"
        sql_content = limitstr + sql_content
#
    queries.append(sql_count_content)
    queries.append(sql_content)
    queries.append(conn_str)
    return queries
#
def rdbms_unloader(table_name,keys,sql_query,src_or_tgt,gen_csv,count_val):
    print("Unloading data from "+src_or_tgt+"...")
#    
    if sql_query[2].find('Oracle') < 0:
        cnxn = pyodbc.connect(sql_query[2])
    else:
        cnxn = sql_query[2]
#
    crsr = cnxn.cursor()
    if (count_val == 'Y'):
        crsr.execute(sql_query[0])
        full_row_count = crsr.fetchone()[0]
    else:
        full_row_count = 0
#    
    qry_start_now = datetime.datetime.now()
    print("Query Retrieving Data - Execution Start Time   : "+ str(qry_start_now))
    crsr.execute(sql_query[1])
    qry_end_now = datetime.datetime.now()
    print("Query Retrieving Data - Execution End Time     : "+ str(qry_end_now))
    Qry_time_elapsed = qry_end_now-qry_start_now
    print("Query Retrieving Data - Execution Elapsed Time : " + str(Qry_time_elapsed))
    desc=crsr.description
    result = list()
    column_names = list()
#
    for i in desc:
        column_names.append(i[0])
#
    result.append(column_names)
    rows = crsr.fetchall()
#
    for row in rows:
        result.append(row)
#
    if (gen_csv == 'Y'):
        datafileName = "C:/Demo/Validation_Tool/Export/"+table_name+"_"+src_or_tgt+'_data.csv'
        with open(datafileName, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#
            for row in result:
                csvwriter.writerow(row)
#
    src_data = [
        dict(zip([str(col[0]).lower() for col in desc], row))
        for row in rows
        ]
    row_count = len(src_data)
    formatted_src_data = {}
#
    for rec in src_data:
        key = "_".join([str(rec[str.lower(k)]).rstrip() for k in keys[table_name]])
        formatted_src_data[key] = rec
#
    src_data = None
    cnxn.close()
    return formatted_src_data,row_count,full_row_count