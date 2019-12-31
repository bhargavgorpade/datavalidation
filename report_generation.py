from config_code import get_target_connection,get_source_db,get_target_db,get_source_table_name,embedded_collection_list,get_row_count,get_source_db_type,get_target_db_type
from rdbms_unloader import collection_exist
import datetime
import webbrowser
import os
#
trgt_conn_str = get_target_connection()
trgt_db = get_target_db()
src_db = get_source_db()
#
global src_type
global trgt_type
global src_count
global trgt_count
global count_val
count_val=""
#
src_type = get_source_db_type()
trgt_type = get_target_db_type()
#   
def data_success(table_name,report_res):
#
    if(report_res[table_name][0][3] == 0 and report_res[table_name][0][4] == 0):
        return "Success"
    else:
        return "Failed"
#
def overall_success(table_name,report_res,count_val):
    cnt_success=count_success(table_name,count_val)
    dat_success=data_success(table_name,report_res)
#
    if cnt_success == "COUNT MATCHED" and dat_success == "Success":
        res = "Success"
    else:
        res = "Failed"
#
    return res
#
def no_of_mismatch(table_name,field_name,report_res):
#
    if(field_name in report_res[table_name][1].keys()):
        res = report_res[table_name][1][field_name]
    else:
        res = 0
#
    return res
#
def src_mismatch_sample(table_name,field_name,report_res):
#
    if(field_name in report_res[table_name][1].keys()):
        res = report_res[table_name][2][field_name][0]
    else:
        res = ""
#
    return res
#
def trg_mismatch_sample(table_name,field_name,report_res):
#
    if(field_name in report_res[table_name][1].keys()):
        res = report_res[table_name][2][field_name][1]
    else:
        res = ""
#
    return res
#
def key_mismatch_sample(table_name,field_name,report_res):
#
    if(field_name in report_res[table_name][1].keys()):
        res = report_res[table_name][2][field_name][2]
    else:
        res = ""
#
    return res
#
def count_success(table_name,count_val):
#
    count = get_row_count()
    src_count = str(count[0])
    trgt_count = str(count[1])
#
    if (count_val == 'Y'):
#
        if trgt_count == src_count:
#
            if int(src_count) != 0: 
                res = "COUNT MATCHED"
            else:
                res = "ZERO COUNT"
#
        else:
            res = "DIFFERENCE IN COUNT"
#
    else:
        res = "COUNT CHECK IGNORED"
#
    return res
#
def overall_table(report_res,count_val):
    res = [[get_source_table_name(tab),tab] + [res[0][5],res[0][6]] + res[0][:5] + [count_success(tab,count_val),data_success(tab,report_res),overall_success(tab,report_res,count_val)] for tab,res in report_res.items() if collection_exist(trgt_conn_str,tab) == 1]
    res = res + [[tab,get_source_table_name(tab),"","","","","","","","","","NOT MIGRATED"] for tab,res in report_res.items() if collection_exist(trgt_conn_str,tab) != 1]
    return res
#
def table_field_details(table_name,report_res):
    res = [[rec[0]] + [rec[0].replace("__",".")] + rec[1:] + [no_of_mismatch(table_name,rec[0],report_res),trg_mismatch_sample(table_name,rec[0],report_res),src_mismatch_sample(table_name,rec[0],report_res),key_mismatch_sample(table_name,rec[0],report_res)] for rec in report_res[table_name][3]]
    return res
#
def add_config_table(reportname):
    cus_list = embedded_collection_list()
    config_header_db = ['Target DB ('+ trgt_type +')','Source DB ('+ src_type +')']
    config_header = ['Target Entity Name','Target SQL query','Source Entity Name','Source SQL query']
    out_res = "<BR><BR><B>DB Configuration Details:</B><BR><TABLE border='1'><TR bgcolor='#ccccb3'><TH>"+"</TH><TH>".join(config_header_db) +"</TH></TR>"
    out_res = out_res+"<TR align='left' bgcolor='#f5f5f0'><TD>"+ trgt_db +"</TD><TD>"+ src_db +"</TD></TR></TABLE>"
    out_res = out_res+"<BR><BR><B>Table/Query Details:</B><BR><TABLE border='1'><TR bgcolor='#ccccb3'><TH>"+"</TH><TH>".join(config_header) +"</TH></TR>"
    tar_table = reportname
    src_table = cus_list[reportname][2]
    tar_query=str("Target Query")
    src_query=str("Source Query")
    out_res = out_res+"<TR align='left' bgcolor='#f5f5f0'><TD>"+ tar_table +"</TD><TD><pre>"+ tar_query +"</pre></TD><TD>"+ src_table +"</TD><TD><pre>"+ src_query +"</pre></TD></TR></TABLE>"
    return out_res
#
def html_report_generation(report_res,reportname,tablekey,count_val):
    count = get_row_count()
    src_count = str(count[0])
    trgt_count = str(count[1])
    dttm_string_now = datetime.datetime.now()
    dttm_string = dttm_string_now.strftime("%d_%b_%Y_%H_%M_%S")
    out_file_name = "C:/Demo/Validation_Tool/Output/Report_"+ reportname +'_'+ dttm_string +".html"
    issue_file_name = "C:/Demo/Validation_Tool/Output/issues.csv"
    report_file_name = "C:/Demo/Validation_Tool/Output/report.csv"
    out_file_issues = open(issue_file_name,"a")
    out_file_report = open(report_file_name,"a")
    overal_header = ['SOURCE ENTITY', 'TARGET ENTITY', "SOURCE RECORD COUNT", "TARGET RECORD COUNT", "SOURCE RECORD COUNT (DISTINCT)", "TARGET RECORD COUNT (DISTINCT)", "MATCHED RECORD COUNT", "SOURCE DIFFERENCE COUNT", "TARGET DIFFERENCE COUNT", 'COUNT CHECK', 'DATA CHECK', 'OVERALL STATUS']
    field_header = ['TARGET FIELD', 'SOURCE FIELD', 'FIELD EXIST IN TARGET', 'FIELD EXIST IN SOURCE', 'TARGET DATATYPE', 'SOURCE DATATYPE', "TARGET DATALENGTH", "SOURCE DATALENGTH", 'FIELD VALIDATION STATUS', "MISMATCH COUNT", 'TARGET MISMATCH SAMPLE', 'SOURCE MISMATCH SAMPLE','KEY ('+ tablekey +')']
    out_file = open(out_file_name,"w")
    out_file.write("<HTML><TITLE>TABLE VALIDATION REPORT</TITLE><BODY><B><U><CENTER><H1>TABLE VALIDATION REPORT</H1></CENTER></U></B>")
#
    out_file.write(add_config_table(reportname))
#   
    if (count_val == 'Y'):
        out_file.write("<BR><BR><B>SOURCE COUNT : "+ src_count +"</B><TABLE border='1'>")
        out_file.write("<BR><BR><B>TARGET COUNT : "+ trgt_count +"</B><TABLE border='1'>")
#
    out_file.write("<BR><BR><B>Validation Details:</B><TABLE border='1'>")
    out_file.write("<TR bgcolor='#ccccb3'><TH>"+"</TH><TH>".join(overal_header) +"</TH></TR>")
#
    for rec in overall_table(report_res,count_val):
        out_file.write(("<TR align='center' bgcolor='#f5f5f0'><TD>"+"</TD><TD>".join([str(elm).upper() for elm in rec]) +"</TD></TR>").replace("NOT MIGRATED",'<p style="color:red";><b>NOT MIGRATED</b></p>').replace("ZERO COUNT",'<p style="color:green";><b>ZERO COUNT</b></p>').replace("COUNT CHECK IGNORED",'<p style="color:blue";><b>COUNT CHECK IGNORED</b></p>').replace("DIFFERENCE IN COUNT",'<p style="color:red";><b>DIFFERENCE IN COUNT</b></p>').replace("FAILED",'<p style="color:red";><b>FAILED</b></p>').replace("SUCCESS",'<p style="color:green";><b>SUCCESS</b></p>').replace("COUNT MATCHED",'<p style="color:green";><b>COUNT MATCHED</b></p>'))
        out_file_report.write("\n"+",".join([str(elm).upper() for elm in rec]))
        out_file_report.write(","+ tablekey.replace(',','|'))
    out_file.write("</TABLE>")
#
    for key in report_res.keys():
        if collection_exist(trgt_conn_str,key) == 1:
            out_file.write("<BR><BR><B><U><LEFT>FIELD DETAILS OF "+ key.upper() +":</LEFT></U></B>")
            out_file.write("<BR><TABLE border='1'>")
            out_file.write("<TR bgcolor='#ccccb3'><TH>"+"</TH><TH>".join(field_header) +"</TH></TR>")
#
            for rec in table_field_details(key,report_res):
                if rec[8] == 'Failed' or rec[9] > 0:
                    rec[8] = 'Failed'
                    out_file.write(("<TR align='center' bgcolor='#f5f5f0'><TD>"+"</TD><TD>".join([str(elm).upper() for elm in rec]) +"</TD></TR>").replace("FAILED",'<p style="color:red";><b>FAILED</b></p>').replace("SUCCESS",'<p style="color:green";><b>SUCCESS</b></p>'))
                    out_file_issues.write("\n"+ reportname +"," +",".join([str(elm).upper() for elm in rec]))
                    out_file_issues.write(","+ tablekey.replace(',','|'))
            out_file.write("</TABLE>")
    out_file.write("</BODY></HTML>")
    out_file.close()
    webbrowser.open('file://' + os.path.realpath(out_file_name),new=2)