# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 14:47:28 2019

@author: msivasuriy
"""
import matplotlib.pyplot as plt
import matplotlib.dates as md
import sys
import pandas as pd
from datetime import datetime
import numpy as np
from matplotlib.ticker import FixedLocator, FixedFormatter


def plural(n):
    if(n>1):
       return 's'
    else:
       return ''
    
def spacing(fig,ax,length):
    print("Length in spcaing"+str(length))
    if(length<=8):
        return
    else:
        every_nth = length//8
        print(every_nth)
        for n, label in enumerate(ax.xaxis.get_ticklabels()):
            if n % every_nth != 0:
                label.set_visible(False)
                
if __name__ == '__main__':     
    print("Starting the program")
    file=sys.argv[1]
    sheetName=sys.argv[2]
    Day=int(sys.argv[3])
    TZ=str(sys.argv[4])
    NumberofDays=int(sys.argv[5])
    fileName="C:/Demo/Validation_Tool/Input/"+file
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
            dt1 = [str(dt1).replace("T"," ") for dt1 in tdf1['TIME']]
            curList=[datetime.strptime(d,"%Y-%m-%d %H:%M:%S") for d in dt1]
            print(curList)
            print("I value: "+str(i))
            if(i==0):
                temptdf=tdf1
                report_date=temptdf.RunDate.unique()
                print(report_date[0])
                j=1
                fullList=curList
                while(j<NumberofDays):
                    print("Looping for "+str(j))
                    tdf2 = tdf[tdf.Day == (Day-j)]
                    dt2 = [str(dt2).replace("T"," ") for dt2 in tdf1['TIME']]
                    prevList = [datetime.strptime(d,"%Y-%m-%d %H:%M:%S") for d in dt2]
                    fullList=fullList+prevList
                    print(curList)
                    j=j+1
                x=list(set(fullList))
                x.sort()
                y=[None] * len(x)
            print(fullList)
            if(i>0):  
                plt.plot(curList,tdf1['SOURCE LATENCY'].tolist(),linestyle='dashed',label = "SOURCE LATENCY "+ "Day "+str(Day-i))
                plt.plot(curList,tdf1['TARGET LATENCY'].tolist(),linestyle='dashed',label = "TARGET LATENCY "+ "Day "+str(Day-i))
                plt.plot(curList,tdf1['HANDLING LATENCY'].tolist(),linestyle='dashed',label = "HANDLING LATENCY "+ "Day "+str(Day-i))
            else:
                fig, ax = plt.subplots()
                plt.plot(x,y)
                #spacing(fig,ax,len(fullList))
                plt.plot(curList,tdf1['SOURCE LATENCY'].tolist(),label = "SOURCE LATENCY "+ "Day "+str(Day))
                plt.plot(curList,tdf1['TARGET LATENCY'].tolist(),label = "TARGET LATENCY "+ "Day "+str(Day))
                plt.plot(curList,tdf1['HANDLING LATENCY'].tolist(),label = "HANDLING LATENCY "+ "Day "+str(Day))
            i=i+1
        #for n, label in enumerate(ax.xaxis.get_ticklabels()):      
        plt.subplots_adjust(bottom=0.2)
        plt.xticks(rotation=90)
        plotTitle=" LATENCY Graph for Day "+str(Day)+"["+str(report_date[0])[:10]+"] for "+str(NumberofDays)+" Day"+plural(NumberofDays)
        plt.title(plotTitle)
        #ax.set_xticks(ax.get_xticks()[::])
        #plt.set_xticks(curList, minor=True)
        #ax.set_xlim(curList[0], curList[-1])
#        curlist1=[]
#        for i in 
#        x_formatter = FixedFormatter(curList)
#        x_locator = FixedLocator(curList)
#        ax.xaxis.set_major_formatter(x_formatter)
#        ax.xaxis.set_major_locator(x_locator)
        #plt.figure(figsize=(4, 3), dpi=70)
        ax = plt.gca()
        ticks_to_use = curList[::100]
        labels = [ i.strftime("%m-%d %H:%M") for i in ticks_to_use ]
        ax.set_xticks(ticks_to_use)
        ax.set_xticklabels(labels)
        #plt.gcf().autofmt_xdate()
        plt.xlabel('x - axis - Start Time (Not to Scale)') 
        # naming the y axis 
        plt.ylabel('y - axis - LATENCY (seconds) (Not to Scale)') 
        plt.legend(loc='upper right')
        #plt.legend()
        plt.savefig("C:\Demo\Validation_Tool\plots\Output\plot_"+plotTitle+".png",dpi=1000, bbox_inches='tight')
        plt.show()
        plt.close('all')