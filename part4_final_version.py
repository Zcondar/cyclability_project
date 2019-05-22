import PyPDF2
import codecs
from urllib.request import urlopen
from bs4 import BeautifulSoup
import sys
import warnings
if not sys.warnoptions:
    warnings.simplefilter("ignore")
totalstr=''
for i in range(8):
    path='text{}.pdf'.format(i+1)
    test=PyPDF2.PdfFileReader(path)
    maxpage=test.numPages
    li=[]
    for i in range(maxpage):
        li.append(test.getPage(i))
    for i in range(maxpage):
        totalstr+=li[i].extractText()
webpath=[]
webpath.append("https://www.sydney.com/things-to-do/adventure-and-sport/cycling")
webpath.append("https://www.campbelltown.nsw.gov.au/ServicesandFacilities/BicycleEducationCentre")
webpath.append("https://www.bikehiresydneyolympicpark.com.au/")
webpath.append("http://www.sydneycycleways.net/resources/sydney-rides/")
webpath.append("https://www.cityofsydney.nsw.gov.au/explore/getting-around/cycling")
webpath.append("https://www.cityofsydney.nsw.gov.au/explore/getting-around/cycling/dockless-bike-sharing")
webpath.append("https://whatson.cityofsydney.nsw.gov.au/events/free-bike-tune-ups-surry-hills")
webpath.append("https://concreteplayground.com/sydney/travel-leisure/leisure/the-ten-best-bike-rides-in-sydney")
webpath.append("http://sydneycyclingclub.org.au/tours/")
webpath.append("https://en.wikipedia.org/wiki/Cycling_in_Sydney")
webpath.append("http://www.sydneycyclist.com/")
webpath.append("https://www.mapmyride.com/au/sydney-new-south-wales/")
webpath.append("https://edition.cnn.com/travel/article/sydney-best-bike-paths/index.html")
webpath.append("https://wiki.openstreetmap.org/wiki/Sydney_Cycle_Routes")
webpath.append("https://www.bikenorth.org.au/")
webpath.append("https://theculturetrip.com/pacific/australia/articles/the-most-spectacular-cycling-routes-near-sydney-australia/")
st=''
for i in webpath:
    url =i
    handle_url = urlopen(url)
    bs = BeautifulSoup(handle_url, 'lxml')
    st+=bs.get_text()
totalstr+=st

text_file = codecs.open("Output.txt", "w","utf-8")
text_file.write(totalstr)
text_file.close()

import os
from nltk.corpus import stopwords 
from math import sqrt
from math import ceil
import spacy
from spacy import displacy
from collections import Counter
import pandas as pd

st=""
file=open("Output.txt","r")
file.seek(0,os.SEEK_END)
file_length=file.tell()
file.seek(0,os.SEEK_SET)
count=0
while count<file_length:
    try:
        st+=file.read(1)
    except:
        continue
    count+=1
stop = set(stopwords.words('english')) 
filter_st= [word for word in st.split(' ') if word not in stop]
st=''
for i in filter_st:
    st+=i.replace("\n","")+" "
 nlp = spacy.load('en_core_web_sm')
count=1
st_list=[]
if (len(st)>999999):
    count=ceil(len(st)/999999)
len_st=len(st)
for i in range(count):
    try:
        st_list.append(st[i*999999:(i+1)*999999])
    except:
        st_list.append(st[i*999999:len_st])
doc_list=[]
for i in range(count):
    doc_list.append(nlp(st_list[i]))
doc_ents=[]
for i in range(count):
    doc_ents+=list(doc_list[i].ents)

maybe_placename=[]
for i in doc_ents:
    if i.label_ not in ['TIME','QUANTITY','DATE','LANGUAGE','MONEY']:
        maybe_placename.append(str(i))

data=pd.read_csv("StatisticalAreas.csv")
area_name=list(data['area_name'])
district=[]
for i in area_name:
    if '-' not in i:
        district.append(i)
        continue
    to_add=i.split('-')[0]
    if to_add[-1]!=' ':
        district.append(to_add)
    else:
        district.append(to_add[:-1]) 

nlp_score={}
for i in district:
    count=0
    for ii in maybe_placename:
        if ii.lower() in i.lower():
            count+=1
    nlp_score[i]=count
avg=0
for i in nlp_score:
    avg+=nlp_score[i]
avg=avg/len(nlp_score)
for i in nlp_score:
    if nlp_score[i]>avg*2:
        nlp_score[i]=int(sqrt(nlp_score[i]))
        
import csv

def create_column(conn, col_name, table_name, type):
    query = """ALTER TABLE {}
                DROP COLUMN IF EXISTS {}, 
                ADD COLUMN {} {};""".format(table_name, col_name, col_name, type)

    pgexec(conn, query, None, "Created Column " + col_name + " on " + table_name)

def update_column_with_another(conn, col_name, table_name, value):
    query = """UPDATE {}
               SET {} = COALESCE{}""".format(table_name, col_name, value)

    pgexec(conn, query, None, "Update " + col_name + " on " + table_name +" with " + value)

def fix_NULL(item):
    if (item == None):
        return 0
    
    return item


import psycopg2
import csv

def pgconnect():
    try:
        # adding options properties to set the schema
        conn = psycopg2.connect(host='Cyclability.cquxnucnvz8p.ap-southeast-2.rds.amazonaws.com',
                                database='postgres',
                                user='team',
                                password='password',
                                options=f'-c search_path=cyclability')
        print('connected')
    except Exception as e:
        print("unable to connect to the database")
        print(e)
    return conn


def pgexec(conn, sqlcmd, args, msg, silent=False):
    retval = False
    with conn:
        with conn.cursor() as cur:
            try:
                if args is None:
                    cur.execute(sqlcmd)
                else:
                    cur.execute(sqlcmd, args)
                if silent == False:
                    print("success: " + msg)
                retval = True
            except Exception as e:
                if silent == False:
                    print("db error: ")
                    print(e)
    return retval


def pgquery(conn, sqlcmd, args, silent=False):
    """ utility function to execute some SQL query statement
        can take optional arguments to fill in (dictionary)
        will print out on screen the result set of the query
        error and transaction handling built-in """
    retval = False
    result = []
    with conn:
        with conn.cursor() as cur:
            try:
                if args is None:
                    cur.execute(sqlcmd)
                else:
                    cur.execute(sqlcmd, args)
                if silent == False:
                    for record in cur:
                        result.append(record)
                retval = True
            except Exception as e:
                if silent == False:
                    print("db read error: ")
                    print(e)
    return result
    
path=open("StatisticalAreas.csv","r")
reader = list(csv.reader(path))
ans1={}
for i in range(1,len(reader)):
    ii=reader[i][1]
    if '-' not in ii:
        ans1[ii]=nlp_score[ii]
        continue
    to_add=ii.split('-')[0]
    if to_add[-1]!=' ':
        ans1[ii]=nlp_score[to_add]
    else:
        ans1[ii]=nlp_score[to_add[:-1]]
ans={}
for i in range(1,len(reader)):
    name=reader[i][1]
    index=reader[i][0]
    ans[index]=ans1[name]

def update_nlp_score(conn,ans):
    subquery = """SELECT * FROM neighbourhoods;"""
    result = pgquery(conn, subquery, None)
    for i in range(1,len(reader)):
        id_=reader[i][0]
        try:
            nlp_s=ans[id_]
        except:
            nlp_s=0
        
        query = """UPDATE neighbourhoods SET NLP_score = {} WHERE area_id= {};""".format(nlp_s,id_)
        pgexec(conn, query, None, "update " + str(nlp_s) + " to " + str(name))
        
if __name__ == "__main__":
    conn = pgconnect()
    create_column(conn,"NLP_score","neighbourhoods","DOUBLE PRECISION")
    create_column(conn, "population_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "dwelling_density", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "service_balance", "neighbourhoods", "DOUBLE PRECISION")
    create_column(conn, "bikepod_density", "neighbourhoods", "DOUBLE PRECISION")
    update_column_with_another(conn, "population_density", "neighbourhoods", "(population / land_area)")
    update_column_with_another(conn, "dwelling_density", "neighbourhoods", "(number_of_dwellings / land_area)")
    update_nlp_score(conn,ans)
    update_service_balance(conn)
    conn.close()
