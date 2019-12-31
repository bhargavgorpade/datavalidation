# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 14:47:28 2019

@author: bgorpade
"""
import re 
import openpyxl 
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

def spacing(fig,ax,length):
    print("Length in spcaing"+str(length))
    if(length<=8):
        return
    else:
        every_nth = length//8
        print(every_nth)
        for n, label in enumerate(ax.xaxis_date().get_ticklabels()):
            if n % every_nth != 0:
                label.set_visible(False)

def plotting(file_path,Sheet,Day,nod):     
    print("Starting the plot")
    file=file_path
    sheetName=Sheet
    Day=int(Day)
    #TZ=str(sys.argv[4])
    NumberofDays=int(nod)
    fileName=file
    xls_file = pd.ExcelFile(fileName)
    df = xls_file.parse(sheetName)
    #print("Execution Starts")
    #print(df)
    #table_list = df.Table.unique()
    if 1==1:
        tdf = df 
        i=0
        #print(table)
        while(i<NumberofDays):
            print(str(Day-i))
            tdf1 = tdf[tdf.Day == (Day-i) ]
            dt1 = [str(dt1).replace("T"," ") for dt1 in tdf1['Time']]
            curList=[datetime.strptime(d,"%Y-%m-%d %H:%M:%S ") for d in dt1]
            print(curList)
            print("I value: "+str(i))
            if(i==0):
                temptdf=tdf1
                report_date=temptdf.Rundate.unique()
                print(report_date[0])
                j=1
                fullList=curList
                while(j<NumberofDays):
                    print("Looping for "+str(j))
                    tdf2 = tdf[tdf.Day == (Day-j)]
                    dt2 = [str(dt2).replace("T"," ") for dt2 in tdf2['Time']]
                    prevList = [datetime.strptime(d,"%Y-%m-%d %H:%M:%S ") for d in dt2]
                    fullList=fullList+prevList
                    print(curList)
                    j=j+1
                x=list(set(fullList))
                x.sort()
                y=[None] * len(x)
            print(fullList)
            if(i>0):  
                plt.plot(curList,tdf1['Source Latency'].tolist(),linestyle='dashed',label = "Source Latency "+ "Day "+str(Day-i))
                plt.plot(curList,tdf1['Target Latency'].tolist(),linestyle='dashed',label = "Target Latency "+ "Day "+str(Day-i))
                plt.plot(curList,tdf1['Handling Latency'].tolist(),linestyle='dashed',label = "Handling Latency "+ "Day "+str(Day-i))
            else:
                fig, ax = plt.subplots()
                print(fig,ax)
                plt.plot(x,y)
                #spacing(fig,ax,len(fullList))
                plt.plot(curList,tdf1['Source Latency'].tolist(),label = "Source Latency "+ "Day "+str(Day))
                plt.plot(curList,tdf1['Target Latency'].tolist(),label = "Target Latency "+ "Day "+str(Day))
                plt.plot(curList,tdf1['Handling Latency'].tolist(),label = "Handling Latency "+ "Day "+str(Day))
            i=i+1
        #for n, label in enumerate(ax.xaxis.get_ticklabels()):      
        plt.subplots_adjust(bottom=0.2)
        plt.xticks(rotation=90)
        plotTitle=" LATENCY Graph for Day "+str(Day)+"["+str(report_date[0])[:10]+"]"
        plt.title(plotTitle)
        ax = plt.gca()
        ticks_to_use = curList[::50]
        labels = [ i.strftime("%m-%d %H:%M") for i in ticks_to_use ]
        ax.set_xticks(ticks_to_use)
        ax.set_xticklabels(labels)
        #plt.gcf().autofmt_xdate()
        plt.xlabel('x - axis - Start Time (Not to Scale)') 
        # naming the y axis 
        plt.ylabel('y - axis - LATENCY (seconds) (Not to Scale)') 
        plt.legend(loc='upper right')
        #plt.legend()
        plt.savefig("C:\Demo\Validation_Tool\Output\plot_"+plotTitle+"_"+str(Day)+".png",dpi=1000, bbox_inches='tight')
        plt.show()
        plt.close('all')
        
#def set_Day()

if __name__ == '__main__':     
    print("Starting the program")
    file=sys.argv[1]
    #day=sys.argv[2]
    fileName="C:/Demo/Validation_Tool/Input/"+file
    print(file)
    fname=file
    df = pd.DataFrame(columns=['Day','Time','Source Latency','Target Latency','Handling Latency','Rundate'])
    book=openpyxl.Workbook()
    sheet = book.active
    header=['TIME','SOURCE LATENCY','TARGET LATENCY','HANDLING LATENCY','RunDate']
    sheet.append(header)
    f=open(fileName,"r")
    new=open(r"C:\demo\Validation_Tool\Output\temp.txt","w")
    f1=f.readlines()
    for x in f1:
        if '(replicationtask.c:2992)' in x:
            new.write(x)
    new.close()
    new1=open(r"C:\demo\Validation_Tool\Output\temp.txt","r")
    newf=new1.readlines()
    ec=""
    dt=""
    for line in newf:
        li=[]
        #ec=""
        date=""
        src=""
        trgt=""
        hand=""
        dt=1
        if '(replicationtask.c:2992)'in line:
            print(line)
            date=re.search(r'(?<=: ).+(?=\[PE)', line).group()
            print(date)
            src=re.search(r'(?<=Source latency ).+(?= seconds, T)', line).group()
            print(src)
            trgt=re.search(r'(?<=Target latency ).+(?= seconds, H)', line).group()
            print(trgt)
            hand=re.search(r'(?<=Handling latency ).+(?= seconds )', line).group()
            print(hand)
#        elif 'db2i_endpoint_capture' in line:
#            print("LINE :",line)
#            #ec="INSIDE"
#            ec=re.search(r"(?<=receiver ').+(?='  \(db2i)", line).group()
#            dt=re.search(r'(?<=: ).+(?=\[SOUR)', line).group()
#        else:
#            ec=""
        li.append(dt)
#        li.append(dt)
#        li.append(ec)
        li.append(date)
        li.append(src)
        li.append(trgt)
        li.append(hand)
        li.append(datetime.today().strftime('%Y-%m-%d'))
        print(li)
        df.append(li)
        df.loc[len(df)] = li
    #print(df)
    date=df[['Time']]
    def trim(x):
        if x.dtype == object:
            x = x.str.split('T').str[0]
        return(x)
    date = date.apply(trim)
    df[['Rundate']]=date
    date_list = date.Time.unique()
    date_list=date_list.tolist()
    print(type(date_list))
    dayno=[]
    for i in range(0,len(date_list)):
        dayno.append(i+1)
    #print(dayno)
    day_li=[]
    #print(df)
    d_list=list(date)
    for i in date_list:
        ind=date_list.index(i)
        df.loc[df.Rundate == i, 'Day'] = dayno[ind]
    #print(df)
    df.to_excel(r'C:\demo\Validation_Tool\Output\op'+fname+'.xlsx',index=False)
    #book.save(r'C:\demo\Validation_Tool\Output\op'+fname+'.xlsx')
    maxi=max(dayno)
    for i in dayno:
        plotting('C:\demo\Validation_Tool\Output\op'+fname+'.xlsx','Sheet1',i,1)
    



    
            
        
    
