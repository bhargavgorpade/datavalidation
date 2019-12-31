import pandas as pd
import matplotlib.pyplot as plt
import datetime
import matplotlib.dates as md
import sys
from collections import Iterable     
#import openpyxl
#import xlsxwriter
#from pivottablejs import pivot_ui

xls_file = pd.ExcelFile(r'C:\demo\Validation_Tool\plots\Input\20191108.xlsx')
df = xls_file.parse('Sheet1')

def convert_to_str(li):
    new=[]
    for i in li:
        i=str(li)
        i=i[2:7]
        i=i.replace(":",".")
        new.append(i)
    return new

def standardize_len(tl,vl1,vl2,vl3):
    length = []
    length.append(len(tl))
    length.append(len(vl1))
    length.append(len(vl2))
    length.append(len(vl3))
    maxi=max(length)
    if(len(vl1)<maxi):
        for i in range(len(vl1),maxi):
            vl1.append(0)
    if(len(tl)<maxi):
        for i in range(len(tl),maxi):
            tl.append(0)
    if(len(vl2)<maxi):
        for i in range(len(vl2),maxi):
            vl2.append(0)
    if(len(vl3)<maxi):
        for i in range(len(vl3),maxi):
            vl3.append(0)
    return tl,vl1,vl2,vl3

def spacing(fig,ax,length):
    if(length<=8):
        return
    else:
        every_nth = length//8
        #every_nth = 20
        for n, label in enumerate(ax.xaxis.get_ticklabels()):
            if n % every_nth != 0:
                label.set_visible(False)

def flatten(items):
    """Yield items from any nested iterable; see Reference."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x
            
def standardize(full_time_list,time_list,value_list):
    value_new=[None] * len(full_time_list)
    for i in range(0,len(full_time_list)):
        if full_time_list[i] in time_list:
            temp=full_time_list[i]
            a = time_list.index(temp)
            value_new[i]=value_list[a]
        else:
            value_new[i]=None
    return value_new
    

def create_plot(datafr1,datafr2,datafr3,nod,report_day,report_date):
    temp_df1=datafr1
    temp_df2=datafr2
    temp_df3=datafr3
    table_list = datafr1.Table.unique()
    #i='ADMIN.SRVERPTM'
    for i in table_list:
        tdf1 = temp_df1[temp_df1.Table == i]
        tdf2 = temp_df2[temp_df2.Table == i]
        tdf3 = temp_df3[temp_df3.Table == i]
        #print(temp_df)

        #tdf1['Start Time'] = [datetime.datetime.time(d) for d in tdf1['Start Time']]

        tdf1['STARTTIME'] = [d.strftime("%H:%M") for d in tdf1['STARTTIME']] 
        tdf2['STARTTIME'] = [d.strftime("%H:%M") for d in tdf2['STARTTIME']] 
        tdf3['STARTTIME'] = [d.strftime("%H:%M") for d in tdf3['STARTTIME']] 
 

#        tdf2['Start Time'] = [datetime.datetime.time(d) for d in tdf2['Start Time']]
#        tdf3['Start Time'] = [datetime.datetime.time(d) for d in tdf3['Start Time']]
        #tdf1["STARTTIME"]= tdf1["STARTTIME"].astype(str) 
        #tdf2["STARTTIME"]= tdf2["STARTTIME"].astype(str)
        #tdf3["STARTTIME"]= tdf3["STARTTIME"].astype(str)
#        tdf2["Start Time"]= tdf2["Start Time"].astype(str) 
#        tdf3["Start Time"]= tdf3["Start Time"].astype(str) 
        time_list1 = tdf1["STARTTIME"].tolist()
        time_list2 = tdf2["STARTTIME"].tolist()
        time_list3 = tdf3["STARTTIME"].tolist()
        #print(time_list1)
        delta_hyperscale1 = tdf1["ISERV_HYPER_DELTA"].tolist()
        delta_hyperscale2 = tdf2["ISERV_HYPER_DELTA"].tolist()
        delta_hyperscale3 = tdf3["ISERV_HYPER_DELTA"].tolist()
        
        delta_CRNTZ1 = tdf1["ISERV_CRNTZ_DELTA"].tolist()
        delta_CRNTZ2 = tdf2["ISERV_CRNTZ_DELTA"].tolist()
        delta_CRNTZ3 = tdf3["ISERV_CRNTZ_DELTA"].tolist()

        full_time_list=[]
        full_time_list.append(time_list1)
        full_time_list.append(time_list2)
        full_time_list.append(time_list3)

        print("PRINTING FULL TIME LIST")
        full_time_list = list(flatten(full_time_list))
        print(full_time_list)
        full_time_list.sort()
        print(full_time_list)
#        time_list1,delta_hyperscale1,delta_hyperscale2,delta_hyperscale3 = standardize_len(time_list1,delta_hyperscale1,delta_hyperscale2,delta_hyperscale3)
#        time_list1,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3 = standardize_len(time_list1,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3)
#        time_list2,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3 = standardize_len(time_list1,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3)
#        time_list3,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3 = standardize_len(time_list1,delta_CRNTZ1,delta_CRNTZ2,delta_CRNTZ3)
        

        # line 1 points 
        x1 = time_list1
        y1 = delta_hyperscale1 
        #x1=convert_to_str(x1)
        # plotting the line 1 points 
        #plt.figure(figsize=(20,10))
        #plt.locator_params(axis='x', nbins=10)
        fig, ax = plt.subplots()
        y=[None] * len(full_time_list)
        x=full_time_list
        plt.plot(x,y)
        print(x1)
        print(y1)
        y1_new = standardize(full_time_list,x1,y1)
        print(y1_new)
        plt.plot(x1, y1, "Red",label = "HS CurrentDay") 
        spacing(fig,ax,len(full_time_list))
        # line 2 points 
        y4 = delta_CRNTZ1
        # plotting the line 2 points  
        y4_new = standardize(full_time_list,x1,y4)
        plt.plot(x1, y4,"Blue", label = "CRNTZ CurrentDay")

        
        x2=time_list2
        if(nod>1):
            y2 = delta_hyperscale2
            # plotting the line 2 points  
            y2_new = standardize(full_time_list,x2,y2)
            plt.plot(x2, y2,color='Green', linestyle='dashed', label = "HS CurrentDay-1") 
            # line 3 points 
            y5 = delta_CRNTZ2
            # plotting the line 2 points  
            y5_new = standardize(full_time_list,x2,y5)
            plt.plot(x2, y5,color='Orange', linestyle='dashed', label = "CRNTZ CurrentDay-1")
            # line 3 points 
            x3=time_list3
            y3 = delta_hyperscale3
            # plotting the line 2 points 
            if(nod==3):
                y3_new = standardize(full_time_list,x3,y3)
                plt.plot(x3, y3,color='Black', linestyle='dashed', label = "HS CurrentDay-2") 
                # line 3 points 
                y6 = delta_CRNTZ3
                # plotting the line 2 points  l
                y6_new = standardize(full_time_list,x3,y6)
                plt.plot(x3, y6,color='Yellow', linestyle='dashed', label = "CRNTZ CurrentDay-2")
               
            # naming the x axis 
        plt.xlabel('x - axis Start Time in IST') 
        # naming the y axis 
        plt.ylabel('y - axis Count Delta') 
        # giving a title to my graph 
        plt.title(i+" Delta Graph for Day "+str(report_day)+"["+str(report_date)[:10]+"] for "+str(nod)+" Day(s)") 
        
        #plt.xlim(full_time_list[0],full_time_list[-1])
        #plt.gcf().autofmt_xdate()
        #plt.xticks(times,times_str,rotation=45)
        # show a legend on the plot 
        plt.legend() 
        #plt.figure(figsize=(80,40))

        #plt.tight_layout()
        # function to show the plot 
        #plt.show() 
        plt.savefig("C:/Demo/Validation_tool/plots/Output/plot_"+i+"_"+str(report_day)+"_"+str(nod)+".png",dpi=300, bbox_inches='tight')
        plt.show() 
        plt.close('all')

        
if __name__ == '__main__':     
    #report_day=sys.argv[1]
    #no_of_days=int(sys.argv[2])
    
    report_day=int(input("Enter report day : "))
    no_of_days=int(input("Enter no of days : "))

    day_list = df.Day.unique()
    print(day_list)
    curr = int(report_day)
    
    
    
    temp_df1=df[df.Day == curr]
    report_date=temp_df1.RunDate.unique()
    temp_df2=df[df.Day == (curr-1)]
    temp_df3=df[df.Day == (curr-2)]
    
    create_plot(temp_df1,temp_df2,temp_df3,no_of_days,report_day,report_date[0])

        


