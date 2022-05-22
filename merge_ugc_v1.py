# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 11:00:38 2022

@author: Vahid
for merge postgres for UGC
"""



import pandas as pd
import numpy as np
import datetime
import os
import time
import pyodbc
from pyodbc import *
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine


# start = time.time() 

# df0 = pd.DataFrame()
# total = pd.DataFrame()


######################## read data from dabase
connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Armin01")
cursor = connection.cursor()
#########   aparat
aparat = psql.read_sql('select * from public."aparat_metatest1"', connection)
print(len(aparat))


aparat_1000 = aparat[1:1000]
aparat_1000['platform'] = 'aparat'
aparat_1000 = aparat_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

aparat_1000.to_excel(r'D:\python\UGC\sample\aparat.xlsx', index=False)

print('فراخونی محتوای aparat')




############## didstan

didestan = psql.read_sql('select * from public."didestan_metatest1"', connection)
print(len(didestan))
print('فراخونی محتوای didestan')
didestan_1000 = didestan[1:1000]
didestan_1000['platform'] = 'didestan'
didestan_1000['categori'] = didestan_1000['categori_name']

didestan_1000 = didestan_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

didestan_1000.to_excel(r'D:\python\UGC\sample\didestan.xlsx', index=False)



###########  gabeh

gabeh = psql.read_sql('select * from public."gabeh_metatest1"', connection)
print(len(gabeh))
print('فراخونی محتوای gabeh')
gabeh_1000 = gabeh[1:1000]
gabeh_1000['platform'] = 'gabeh'
gabeh_1000['categori'] = gabeh_1000['categori_name']
gabeh_1000 = gabeh_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

gabeh_1000.to_excel(r'D:\python\UGC\sample\gabeh.xlsx', index=False)



############       mihanvideo

mihanvideo = psql.read_sql('select * from public."mihanvideo_metatest1"', connection)
print(len(mihanvideo))
print('فراخونی محتوای mihanvideo')
mihanvideo_1000 = mihanvideo[1:1000]
mihanvideo_1000['categori'] = mihanvideo_1000['categori_name']
mihanvideo_1000['platform'] = 'mihanvideo'
mihanvideo_1000 = mihanvideo_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

mihanvideo_1000.to_excel(r'D:\python\UGC\sample\mihanvideo.xlsx', index=False)


################   mp4

mp4 = psql.read_sql('select * from public."mp4_metatest1"', connection)
print(len(mp4))
print('فراخونی محتوای mp4')
mp4_1000 = mp4[1:1000]
mp4_1000['platform'] = 'mp4'
mp4_1000 = mp4_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

mp4_1000.to_excel(r'D:\python\UGC\sample\mp4.xlsx', index=False)


################   namasha
namasha = psql.read_sql('select * from public."namasha_metatest1"', connection)
print(len(namasha))
print('فراخونی محتوای namasha')

namasha_1000 = namasha[1:1000]
namasha_1000['platform'] = 'namasha'
namasha_1000['categori'] = namasha_1000['categori_name']

namasha_1000 = namasha_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

namasha_1000.to_excel(r'D:\python\UGC\sample\namasha.xlsx', index=False)


################  namayesh

namayesh = psql.read_sql('select * from public."namayesh_metatest1"', connection)
print(len(namasha))
print('فراخونی محتوای namayesh')

namayesh_1000 = namayesh[1:1000]
namayesh_1000['platform'] = 'namayesh'

namayesh_1000 = namayesh_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

namayesh_1000.to_excel(r'D:\python\UGC\sample\namayesh.xlsx', index=False)


################     shabakema

shabakema = psql.read_sql('select * from public."shabakema_metatest1"', connection)
print(len(shabakema))
print('فراخونی محتوای shabakema')

shabakema_1000 = shabakema[1:1000]
shabakema_1000['platform'] = 'shabakema'

shabakema_1000 = shabakema_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

shabakema_1000.to_excel(r'D:\python\UGC\sample\shabakema.xlsx', index=False)



#############  tamasha

tamasha = psql.read_sql('select * from public."tamasha_metatest1"', connection)
print(len(tamasha))
print('فراخونی محتوای tamasha')

tamasha_1000 = tamasha[1:1000]
tamasha_1000['platform'] = 'tamasha'
tamasha_1000 = tamasha_1000[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

tamasha_1000.to_excel(r'D:\python\UGC\sample\tamasha.xlsx', index=False)

frames00 = [aparat_1000, didestan_1000, gabeh_1000, mihanvideo_1000 , mp4_1000, namasha_1000, namayesh_1000, shabakema_1000, tamasha_1000]
adgham0 = pd.concat(frames00)
adgham0.to_excel(r'D:\python\UGC\sample\adgham0.xlsx', index=False)









print('فراخونی محتوای درخواستی')







