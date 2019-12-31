import pyodbc
#import cx_Oracle
#
from config_code import get_metadata_query 
#
def get_metadata_from_table(conn_str,table_name,remove_column):
    res = {}
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
    qry = get_metadata_query(conn_str,tab_schema,tab_name)
    crsr.execute(qry)
    rows = crsr.fetchall()
#
    for col_nm,data_type,len_1,len_2 in rows:
#
        if col_nm not in remove_column:
#
            datyp = data_type
#        
            if (len_2 == None):
#
                if (len_1 != None):
                    leng = len_1
                else:
                    leng = '23'
#
            else:
                leng = len_1+','+len_2
#
            res[str.lower(col_nm)] = {"Datatype":datyp,"length":leng}
    return res
#
def metadata_comparsion(src_meta,trg_meta):
    dist_all_keys = list(set(list(src_meta.keys())+list(trg_meta.keys())))
    res_set = []
#
    for key in dist_all_keys:
        src_key_exist = "No/Only Null Value"
        trg_key_exist = "No/Only Null Value"
        src_datatype = ""
        trg_datatype = ""
        src_data_len = 0
        trg_data_len = 0
        field_valid = "Success"
#
        if key in src_meta.keys():
            src_key_exist = "Yes"
            src_data_len = src_meta[key]["length"]
            src_datatype = src_meta[key]["Datatype"]
#
        if key in trg_meta.keys():
            trg_key_exist = "Yes"
            trg_data_len = trg_meta[key]["length"]
            trg_datatype = trg_meta[key]["Datatype"]
#
#
        if src_key_exist != trg_key_exist or src_datatype != trg_datatype or src_data_len != trg_data_len:
            field_valid = "Failed"
#
        res_set.append([key,trg_key_exist,src_key_exist,trg_datatype,src_datatype,trg_data_len,src_data_len,field_valid])
    return res_set