import pandas as pd
import numpy

xls_file = pd.ExcelFile(r'C:\Users\bgorpade\Documents\Code\sql_queries\20191031.xlsx')
df = xls_file.parse('20191031')
#print(df)

table = df.Table.unique()
#i = 'ADMIN.LSNYTD00'
for i in table:
    temp_df = df[df.Table == i]
    
    time_list = temp_df["Start Time"].tolist()
    delta_hyperscale = temp_df["Delta_HyperScale"].tolist()
    delta_crntz = temp_df["Delta_CRNTZ"].tolist()
    
    temp1 = temp_df[temp_df.Delta_HyperScale == 0]
    #print(temp1)
    temp2 = temp1[temp1.Delta_CRNTZ == 0]
    #print(temp2)
    temp3 = temp2.iloc[:1]
    #temp3['First_Occurence'] = temp3['Start Time'] + temp3['time taken']
    print(temp3)
    cols = [0,1]
    temp4 = temp3[temp3.columns[cols]]
    
    with open (r"C:\Users\bgorpade\Documents\Code\sql_queries\Output\first_occurence.csv",'a',newline='') as filedata:                            
          temp4.to_csv(filedata, header=False,index=False)
          

    
    