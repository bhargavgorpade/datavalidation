# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 11:38:14 2019

@author: bgorpade
"""
import pandas as pd
import glob
import os
import csv

#def column_exists(df,ind):
#    col_check=None
#    if(df['FIELD_EXIST_IN_TARGET'][ind]!=df['FIELD_EXIST_IN_SOURCE'][ind]):
#        if(df['FIELD_EXIST_IN_TARGET'][ind] == 'YES'):
#            col_check=df['TARGET_FIELD'][ind]+" Column is present in TRGT but not in SRC."
#        else:
#            col_check=df['TARGET_FIELD'][ind]+" Column is present in SRC but not in TRGT."
#    if col_check:
#        return col_check
#    else:
#        return ""
    
def column_exists_new(df):
    #print("COLUMN EXISTS ULLA POIDCHI")
    SourceNotTarget=[]
    TargetNotSource=[]
    for ind in df.index:
        if(df['FIELD_EXIST_IN_TARGET'][ind]!=df['FIELD_EXIST_IN_SOURCE'][ind]):
            if(df['FIELD_EXIST_IN_SOURCE'][ind]!="NO/ONLY NULL VALUE"):
                SourceNotTarget.append(df['TARGET_FIELD'][ind])
            elif(df['FIELD_EXIST_IN_TARGET'][ind]!="NO/ONLY NULL VALUE"):
                TargetNotSource.append(df['TARGET_FIELD'][ind])
    print(SourceNotTarget,TargetNotSource)
    return SourceNotTarget,TargetNotSource
            
#def datatype_check(df):
#    dt_check=""
#    for ind in df.index:
#        if(df['TARGET_DATATYPE'][ind]!=df['SOURCE_DATATYPE'][ind]):
#            #dt_check=" "+"Datatype Mismatch in "+str(df['TARGET_FIELD'][ind])+" Column - "+str(df['SOURCE_DATATYPE'][ind])+" in SRC, "+str(df['TARGET_DATATYPE'][ind])+" in TRGT."
#            dt_check=dt_check+str(df['TARGET_FIELD'][ind])+", "
#    if dt_check:
#        return dt_check
#    else:
#        return ""
    
def datatype_check_new(df):
    Combin=[]
    for ind in df.index:
        if(df['TARGET_DATATYPE'][ind]!=df['SOURCE_DATATYPE'][ind]):
            Combin.append([df['SOURCE_DATATYPE'][ind],df['TARGET_DATATYPE'][ind]])
    unique_data = [list(x) for x in set(tuple(x) for x in Combin)]
    print("Combinations : ",unique_data)
    ColumnsInCombin=[""]*len(unique_data)
    #myDict.setdefault(key, [])
    for ind in df.index:
        if(df['TARGET_DATATYPE'][ind]==None or df['SOURCE_DATATYPE'][ind]==None):
            return ""
        else:
            if(df['TARGET_DATATYPE'][ind]!=df['SOURCE_DATATYPE'][ind]):
                temp=[]
                temp.append(df['SOURCE_DATATYPE'][ind])
                temp.append(df['TARGET_DATATYPE'][ind])
                print("TEMP: ",temp)
                for i in unique_data:
                    if temp == i:
                        inde=unique_data.index(i)
                        ColumnsInCombin[inde]=ColumnsInCombin[inde]+" "+str(df['TARGET_FIELD'][ind])+", "
                    
    DtStr=""
    for ind in range(0,len(ColumnsInCombin)):
        DtStr=DtStr+"Datatype mismatch in "+ColumnsInCombin[ind]+" Column(s) - "+str(unique_data[ind][0])+" in SRC, "+str(unique_data[ind][1])+" in TRGT "
    DtStr="* "+DtStr+"\n"
    return DtStr
                    
            
        

def length_check(df,ind):
    len_check=None
    if(df['TARGET_DATALENGTH'][ind]!=df['SOURCE_DATALENGTH'][ind]):
        print(df['SOURCE_DATALENGTH'][ind])
        len_check=" "+"Length Mismatch in "+df['TARGET_FIELD'][ind]+" Column - "+str(df['SOURCE_DATALENGTH'][ind])+" in SRC, "+str(df['TARGET_DATALENGTH'][ind])+" in TRGT."
    if len_check:
        return len_check
    else:
        return ""

def data_check(df,ind):
    last=df[df.columns[-1]]
    last_name=df.columns[-1]
    d_check=None
    if(str(df['TARGET_MISMATCH_SAMPLE'][ind]).strip()!=str(df['SOURCE_MISMATCH_SAMPLE'][ind]).strip()):
        d_check=" "+"Data Mismatch in "+str(df['TARGET_FIELD'][ind])+" Column - "+str(df['SOURCE_MISMATCH_SAMPLE'][ind])+" in SRC, "+str(df['TARGET_MISMATCH_SAMPLE'][ind])+" in TRGT for "+last_name+" - "+str(last[ind])
    if d_check:
        return d_check
    else:
        return ""


path = 'C:\demo\Validation_Tool\Output'
with open('C:\demo\Validation_Tool\Output\my_csv.csv', 'a',newline='') as f:
    for filename in glob.glob(os.path.join(path, '*.html')):

        li=[]
        print(filename)
        df = pd.read_html(filename)
        df1=pd.DataFrame(df[1])
        df=pd.DataFrame(df[3]) 
        table_name=df1[["Target Entity Name"]]
        lst=table_name.values.tolist()
        lst=lst[0]
        lst=lst[0]
        df.columns = [c.replace(' ', '_') for c in df.columns]
        #writer = ExcelWriter('Pandas-Example2.xlsx')
        #df.to_csv('C:\demo\Validation_Tool\Output\my data.csv')
        final_str=""
        SourceNotTarget, TargetNotSource = column_exists_new(df)
        #datatype_check_new(df)
        SourceNotTargetStr=""
        TargetNotSourceStr=""
        if len(SourceNotTarget)!=0: 
            SourceNotTargetStr = ' '.join([str(elem) for elem in SourceNotTarget])
            SourceNotTargetStr = SourceNotTargetStr + " Column(s) present in SRC but not in TRGT"
        if len(TargetNotSource)!=0:
            TargetNotSourceStr = ' '.join([str(elem) for elem in TargetNotSource])
            TargetNotSourceStr = TargetNotSourceStr + " Column(s) present in TRGT but not in SRC"
        #print("CONTROL IRUKA ILLAYA")
        if len(SourceNotTarget)==0 and len(TargetNotSource)==0:
            final_str=""    
            dt_final=datatype_check_new(df)
            final_str=final_str + dt_final
        else:
            infer_column=SourceNotTargetStr+" "+TargetNotSourceStr
            final_str="* "+infer_column+"\n"
            print("FINAL : ",final_str)
            dt_final=datatype_check_new(df)
            final_str=final_str + dt_final
        for ind in df.index:
            infer=""
            #col_check=column_exists(df,ind)
            #dt_check=datatype_check(df)
            len_check=length_check(df,ind)
            d_check=data_check(df,ind)
            infer_all=len_check+d_check
            if infer_all!="":
                 infer="* "+infer_all+"\n"
                 final_str=final_str+infer
                 
             #print(infer)
        print(final_str)
        li.append(lst)
        li.append(final_str)
        wr = csv.writer(f, dialect='excel')
        wr.writerow(li)
        
        
        
         




