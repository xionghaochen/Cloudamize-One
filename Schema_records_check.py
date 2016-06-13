'''
Created on Jun 2, 2016

@author: walter
'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-

' The first Python project '

__author__ = 'Walter Xiong'

import sys
import getopt
import psycopg2

def main(argv):
    host1=''
    dbname1=''
    port1=''
    user1=''
    password1=''
    host2=''
    dbname2=''
    port2=''
    user2=''
    password2=''
        
    try:
        opts,args=getopt.getopt(argv,"h:d:p:u:w:H:D:P:U:W",["host1=", "dbname1=", "port1=", "user1=", "password1=","host2=", "dbname2=", "port2=", "user2=", "password2="])
    except getopt.GetoptError:
        sys.exit()
        
    for key,value in opts:
        if key in ('-h','--host1'):
            host1=value
        if key in ('-d','--dbname1'):
            dbname1=value
        if key in ('-p','--port1'):
            if value=='':
                port1='5432'
            else:
                port1=value
        if key in ('-u','--user1'):
            if value=='':
                user1='postgres'
            else:
                user1=value
        if key in ('-w','--password1'):
            password1=value
        if key in ('-H','--host2'):
            host2=value
        if key in ('-D','--dbname2'):
            dbname2=value
        if key in ('-P','--port2'):
            if value=='':
                port2='5432'
            else:
                port2=value
        if key in ('-U','--user2'):
            if value=='':
                user2='postgres'
            else:
                user2=value
        if key in ('-W','--password2'):
            password2=value    
    
    connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2)


def connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2):
    # This function is used to connect to specified database. 
    
    conn_string1= "host=%s dbname=%s port=%s user=%s password=%s"%(host1,dbname1,port1,user1,password1)
    print ("Connecting to database\n\n-->%s\n" %(conn_string1))
    
    conn1=psycopg2.connect(conn_string1)
    cursor1=conn1.cursor()
    
    print ("Database %s is connected!\n"%dbname1)
    
    conn_string2= "host=%s dbname=%s port=%s user=%s password=%s"%(host2,dbname2,port2,user2,password2)
    print ("Connecting to database\n\n-->%s\n" %(conn_string2))
    
    conn2=psycopg2.connect(conn_string2)
    cursor2=conn2.cursor()
    
    print ("Database %s is connected!\n"%dbname2)
    
    print('Which table do you want to compare: ')
    t=input()    
    
    if t!='': 
        cursor1.execute('select count(*) from information_schema.columns where table_name=\'%s\''%t)
        s_columns=cursor1.fetchall()
        
        cursor1.execute('select column_name,data_type from information_schema.columns where table_name=\'%s\''%t)
        s_columns_infor=cursor1.fetchall()
        
        cursor2.execute('select count(*) from information_schema.columns where table_name=\'%s\''%t)
        c_columns=cursor2.fetchall()
        
        cursor2.execute('select column_name,data_type from information_schema.columns where table_name=\'%s\''%t)
        c_columns_infor=cursor2.fetchall()
        
        if schema_compare(t,s_columns,s_columns_infor,c_columns,c_columns_infor):
            print('\nPlease enter a query: ')
            db_q=input()
            if db_q!='':
                cursor1.execute('%s'%db_q)
                s_colnames = [desc[0] for desc in cursor1.description]
                
                cursor1.execute('%s'%db_q)
                s_data=cursor1.fetchall()
                
                cursor2.execute('%s'%db_q)
                c_colnames = [desc[0] for desc in cursor2.description]
                
                cursor2.execute('%s'%db_q)
                c_data=cursor2.fetchall()
                
                select_compare(dbname1,s_data,s_colnames,dbname2,c_data,c_colnames)
                
            else:
                print('\nWrong query')
        else:
            print('\nDone')          
    else:
        print('\nWrong choice!')


def schema_compare(t,s_columns,s_columns_infor,c_columns,c_columns_infor):
        
    x,i,count,sign=0,0,0,0
     
    if c_columns!=s_columns:
        print('The number of columns are not match!\n')
        return False
    else:
        while x<len(s_columns_infor):
            while i<len(c_columns_infor):
                while count<len(c_columns_infor[0]):
                    if s_columns_infor[x][count]==c_columns_infor[i][count]:
                        break
                    else:
                        break
                if s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)<len(c_columns_infor[0]):
                    count=count+1
                elif s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)>=len(c_columns_infor[0]):
                    if (x+1)<len(s_columns_infor):
                        x=x+1
                        i=0
                        count=0
                    else:
                        if sign==0:
                            print('\nThe schema of table \'%s\' in those two databases are completely matching'%t)
                            return True
                        else:
                            return False
                elif s_columns_infor[x][count]!=c_columns_infor[i][count]:
                    if (i+1)<len(c_columns_infor):
                        i=i+1
                        count=0
                    else:
                        print('\nThere is no match column in compared table which is ',s_columns_infor[x])
                        sign=sign+1
                        if (x+1)<len(s_columns_infor):
                            x=x+1
                            i=0
                            count=0
                        else:
                            if sign==0:
                                print('\nThe schema of table \'%s\' in those two databases are completely matching'%t)
                                return True
                            else:
                                return False
                
def select_compare(dbname1,s_data,s_colnames,dbname2,c_data,c_colnames):
    
    x,i,sign,s_count,c_count=0,0,0,0,0
    
    while True:
        if len(s_data)!=len(c_data):
            print('\nThe number of records are not match')
            break
        else:
            if len(s_colnames)==1 and s_colnames[0]=='id':
                print('\nYou should not compare primary key')
                break
            elif len(s_colnames)==1 and s_colnames[0]!='id':
                s_count,c_count=0,0
            elif len(s_colnames)>1:
                for s in s_colnames:
                    if s=='id':
                        s_count,c_count=1,1
                        break
                    else:
                        s_count,c_count=0,0

            while x<len(s_data):
                while i<len(c_data):
                    while s_count<len(s_colnames) and c_count<len(c_colnames):
                        if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)<len(s_colnames):
                            s_count=s_count+1
                            c_count=1
                        elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)>=len(s_colnames):
                            break
                        elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count]:
                            if (i+1)<len(c_data):
                                break
                            else:
                                sign=sign+1
                                print('\nFor database %r, there is no matching record: %r'%(dbname2,s_data[x]))
                                break
                        else:
                            c_count=c_count+1
                    if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)>=len(s_colnames):
                        break
                    elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (i+1)>=len(c_data):
                        break
                    elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (i+1)<len(c_data):
                        i=i+1
                        if len(s_colnames)==1 and s_colnames[0]!='id':
                            s_count,c_count=0,0
                        elif len(s_colnames)>1:
                            for s in s_colnames:
                                if s=='id':
                                    s_count,c_count=1,1
                                    break
                                else:
                                    s_count,c_count=0,0
                    else:
                        break
                if (x+1)<len(s_data):
                    x=x+1
                    i=0
                    if len(s_colnames)==1 and s_colnames[0]!='id':
                        s_count,c_count=0,0
                    elif len(s_colnames)>1:
                        for s in s_colnames:
                            if s=='id':
                                s_count,c_count=1,1
                                break
                            else:
                                s_count,c_count=0,0
                else:
                    if sign==0:
                        print('\nFor database %r, those two tables might be matching'%dbname1)
                        break
                    else:
                        break
            
            x,i=0,0
            
            if len(s_colnames)==1 and s_colnames[0]=='id':
                print('\nYou should not compare primary key')
                break
            elif len(s_colnames)==1 and s_colnames[0]!='id':
                s_count,c_count=0,0
            elif len(s_colnames)>1:
                for s in s_colnames:
                    if s=='id':
                        s_count,c_count=1,1
                        break
                    else:
                        s_count,c_count=0,0
                        
            while i<len(c_data):
                while x<len(s_data):
                    while s_count<len(s_colnames) and c_count<len(c_colnames):
                        if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)<len(c_colnames):
                            c_count=c_count+1
                            s_count=1
                        elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)>=len(c_colnames):
                            break
                        elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count]:
                            if (x+1)<len(s_data):
                                break
                            else:
                                sign=sign+1
                                print('\nFor database %r, there is no matching record: %r'%(dbname1,c_data[i]))
                                break
                        else:
                            s_count=s_count+1
                    if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)>=len(c_colnames):
                        break
                    elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (x+1)>=len(s_data):
                        break
                    elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (x+1)<len(s_data):
                        x=x+1
                        if len(s_colnames)==1 and s_colnames[0]!='id':
                            s_count,c_count=0,0
                        elif len(s_colnames)>1:
                            for s in s_colnames:
                                if s=='id':
                                    s_count,c_count=1,1
                                    break
                                else:
                                    s_count,c_count=0,0
                    else:
                        break
                if (i+1)<len(c_data):
                    i=i+1
                    x=0
                    if len(s_colnames)==1 and s_colnames[0]!='id':
                        s_count,c_count=0,0
                    elif len(s_colnames)>1:
                        for s in s_colnames:
                            if s=='id':
                                s_count,c_count=1,1
                                break
                            else:
                                s_count,c_count=0,0
                else:
                    if sign==0:
                        print('\nFor database %r, those two tables might be matching'%dbname2)
                        break
                    else:
                        break

            break
    if sign==0:
        print('\nThose two tables are totally matching')
    else:
        print('\nThose two tables are not matching')
                
if __name__=='__main__':
    main(sys.argv[1:])