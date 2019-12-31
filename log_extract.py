# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 14:47:28 2019

@author: bgorpade
"""
import re 
import openpyxl 
import sys
from datetime import datetime


if __name__ == '__main__':     
    print("Starting the program")
    file=sys.argv[1]
    day=sys.argv[2]
    fileName="C:/Demo/Validation_Tool/Input/"+file
    print(file)
    fname=file
    book=openpyxl.Workbook()
    sheet = book.active
    header=['Day','TIME','SOURCE LATENCY','TARGET LATENCY','HANDLING LATENCY','RunDate']
    sheet.append(header)
    f=open(fileName,"r")
    new=open(r"C:\demo\Validation_Tool\Output\temp.txt","w")
    f1=f.readlines()
    for x in f1:
        if 'db2i_endpoint_capture' in x:
            if "Starting journal receiver" in x:
                new.write(x)
        elif '(replicationtask.c:2992)' in x:
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
        li.append(day)
#        li.append(dt)
#        li.append(ec)
        li.append(date)
        li.append(src)
        li.append(trgt)
        li.append(hand)
        li.append(datetime.today().strftime('%Y-%m-%d'))
        print(li)
        sheet.append(li)
    
    book.save(r'C:\demo\Validation_Tool\Output\op'+fname+'.xlsx')


    
            
        
    
