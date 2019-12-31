# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 14:47:28 2019

@author: msivasuriy
"""
import matplotlib.pyplot as plt
import matplotlib.dates as md
import sys
import pandas as pd
import datetime
import numpy as np

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
    table_list = df.Table.unique()
    for table in table_list:
        tdf = df[df.Table == table] 
        i=0
        #print(table)
        while(i<NumberofDays):
            print(str(Day-i))
            tdf1 = tdf[tdf.Day == (Day-i) ]
            curList=[d.strftime("%H:%M") for d in tdf1['STARTTIME']]
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
                    prevList = [d.strftime("%H:%M") for d in tdf2['STARTTIME']]
                    fullList=fullList+prevList
                    #print(fullList)
                    j=j+1
                x=list(set(fullList))
                x.sort()
                y=[None] * len(x)

            if(i>0):  
                plt.plot(curList,tdf1['ISERV_HYPER_DELTA'].tolist(),linestyle='dashed',label = str('\u0394')+" HS   "+ "Day "+str(Day-i))
                plt.plot(curList,tdf1['ISERV_CRNTZ_DELTA'].tolist(),linestyle='dashed',label = str('\u0394')+" CRNTZ "+ "Day "+str(Day-i))
            else:
                fig, ax = plt.subplots()
                plt.plot(x,y)
                spacing(fig,ax,len(fullList))
                plt.plot(curList,tdf1['ISERV_HYPER_DELTA'].tolist(),label = str('\u0394')+" HS   "+ "Day "+str(Day))
                plt.plot(curList,tdf1['ISERV_CRNTZ_DELTA'].tolist(),label = str('\u0394')+" CRNTZ "+ "Day "+str(Day))
            i=i+1
        #for n, label in enumerate(ax.xaxis.get_ticklabels()):      
        plt.subplots_adjust(bottom=0.2)
        plt.xticks(rotation=90)
        plotTitle=table+" Delta Graph for Day "+str(Day)+"["+str(report_date[0])[:10]+"] for "+str(NumberofDays)+" Day"+plural(NumberofDays)
        plt.title(plotTitle)
        plt.xlabel('x - axis - Start Time in '+TZ+ ' (Not to Scale)') 
        # naming the y axis 
        plt.ylabel('y - axis - Count Delta (Not to Scale)') 
        plt.legend(loc='upper right')
        #plt.legend()
        plt.savefig("C:\Demo\Validation_Tool\Output\plot_"+plotTitle+".png",dpi=300, bbox_inches='tight')
        plt.close('all')