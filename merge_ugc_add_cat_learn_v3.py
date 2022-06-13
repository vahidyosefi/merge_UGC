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


# aparat_1000 = aparat[1:1000]
aparat['platform'] = 'آپارات'
aparat = aparat[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

# aparat.to_excel(r'D:\python\UGC\sample\aparat28n.xlsx', index=False)

aparat.dtypes

aparat['viwes'] = aparat['viwes'].str.replace(',', '')
aparat['like_count'] = aparat['like_count'].str.replace(',', '')
aparat['viwes'] = aparat['viwes'].astype(str).astype(int)
aparat['follower'] = aparat['follower'].astype(str).astype(int)
aparat['like_count'] = aparat['like_count'].astype(str).astype(int)


aparat_r = aparat
# aparat_r = aparat[['content_name','viwes','like_count','channel_name','follower','crawling_date']]
aparat_f = aparat_r.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')

# aparat_f = aparat_r.sort_values('viwes').drop_duplicates(['content_link'], keep='last')
print(len(aparat_f))
# aparat_f.to_excel(r'D:\python\UGC\sample\aparat28rrr.xlsx', index=False)
print('فراخونی محتوای aparat')


############## didstan



didestan = psql.read_sql('select * from public."didestan_metatest1"', connection)

didestan.dtypes
print(len(didestan))

print('فراخونی محتوای didestan')
# didestan = didestan[1:1000]
didestan['platform'] = 'دیدستان'
didestan['categori'] = didestan['categori_name']

didestan = didestan[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

# didestan.to_excel(r'D:\python\UGC\sample\didestan_rowdata.xlsx', index=False)


####remove dublicates on data didestan

didestan = didestan.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# didestan.to_excel(r'D:\python\UGC\sample\didestan_r.xlsx', index=False)
print(len(didestan))





#####################################################

###########  gabeh

gabeh = psql.read_sql('select * from public."gabeh_metatest1"', connection)
print(len(gabeh))
print('فراخونی محتوای gabeh')
# gabeh_1000 = gabeh[1:1000]
gabeh['platform'] = 'جعبه'
gabeh['categori'] = gabeh['categori_name']
gabeh = gabeh[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]
gabeh.dtypes

# gabeh.to_excel(r'D:\python\UGC\sample\gabeh_row data.xlsx', index=False)


####remove dublicates on data gabeh

gabeh = gabeh.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# gabeh.to_excel(r'D:\python\UGC\sample\gabeh_r.xlsx', index=False)
print(len(gabeh))

####################################################


############       mihanvideo

mihanvideo = psql.read_sql('select * from public."mihanvideo_metatest1"', connection)
print(len(mihanvideo))
print('فراخونی محتوای mihanvideo')
# mihanvideo_1000 = mihanvideo[1:10000]
mihanvideo['categori'] = mihanvideo['categori_name']
mihanvideo['platform'] = 'میهن ویدیو'
mihanvideo = mihanvideo[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

# mihanvideo_1000.to_excel(r'D:\python\UGC\sample\mihanvideo_row data.xlsx', index=False)
mihanvideo.dtypes

####remove dublicates on data mihanvideo

mihanvideo = mihanvideo.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# mihanvideo.to_excel(r'D:\python\UGC\sample\mihanvideo_r.xlsx', index=False)

print(len(mihanvideo))


################################################


################   mp4

mp4 = psql.read_sql('select * from public."mp4_metatest1"', connection)
print(len(mp4))
print('فراخونی محتوای mp4')
# mp4_1000 = mp4[1:1000]
mp4['platform'] = 'mp4'
mp4 = mp4[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]

# mp4.to_excel(r'D:\python\UGC\sample\mp4.xlsx', index=False)


####remove dublicates on data mp4

mp4 = mp4.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# mp4.to_excel(r'D:\python\UGC\sample\mp4_r.xlsx', index=False)

print(len(mp4))


######################################################

################   namasha
namasha = psql.read_sql('select * from public."namasha_metatest1"', connection)
print(len(namasha))
print('فراخونی محتوای namasha')

# namasha_1000 = namasha[1:1000]
namasha['platform'] = 'نماشا'
namasha['categori'] = namasha['categori_name']

namasha = namasha[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]
# namasha.to_excel(r'D:\python\UGC\sample\namasha_row data.xlsx', index=False)

namasha.dtypes
####remove dublicates on data namasha

namasha['viwes'] = 100 * namasha['like_count']
namasha.dtypes
namasha = namasha.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# namasha.to_excel(r'D:\python\UGC\sample\namasha_r.xlsx', index=False)
print(len(namasha))




#########################################################

################  namayesh

namayesh = psql.read_sql('select * from public."namayesh_metatest1"', connection)
print(len(namayesh))
print('فراخونی محتوای namayesh')

# namayesh_1000 = namayesh[1:1000]
namayesh['platform'] = 'نمایش'

namayesh = namayesh[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]
# namayesh.to_excel(r'D:\python\UGC\sample\namayesh_row data_2.xlsx', index=False)
namayesh.dtypes
######
namayesh['viwes'] = namayesh['viwes'].str.replace(',', '')

namayesh['follower'] = namayesh['follower'].str.replace("  آ ‌ ",'')

# np.where(pd.isnull(namayesh))
# namayesh['viwes']=namayesh['viwes'].fillna(0)
# namayesh['follower']=namayesh['follower'].fillna(0)
# namayesh['viwes'][namayesh['viwes'].isnull()] = 0
# namayesh.loc[(namayesh['viwes'] .isnull()) , 'viwes'] = 'nanvalue'
# namayesh.loc[(namayesh['viwes'] .isnull()) , 'viwes'] = 'nanvalue'
# namayesh.loc[(namayesh['content_name'] .isnull()) , 'content_name'] = 'سایر'
# namayesh.dropna(subset = ["content_name"], inplace=True)
# namayesh['content_name']=namayesh['content_name'].fillna(0)


namayesh['content_name'].replace('', 'NO', inplace=True)
# namayesh['viwes'].replace('', 'NO', inplace=True)
namayesh = namayesh[~namayesh.content_name.str.contains("NO")]

namayesh['follower'].replace('', 0, inplace=True)
# namayesh = namayesh[~namayesh.follower.str.contains("NO")]
# namayesh['viwes']=namayesh['viwes'].fillna(0)


# namayesh = namayesh.replace(np.nan, 0)

namayesh['like_count'] = namayesh['like_count'].str.replace(',', '')
namayesh['viwes'] = namayesh['viwes'].astype(str).astype(int)
namayesh['follower'] = namayesh['follower'].astype(str).astype(int)
namayesh['like_count'] = namayesh['like_count'].astype(str).astype(int)

######
###remove dublicates on data namayesh

namayesh = namayesh.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# namayesh.to_excel(r'D:\python\UGC\sample\namayesh_r.xlsx', index=False)

print(len(namayesh))






##########################################################

################     shabakema

shabakema = psql.read_sql('select * from public."shabakema_metatest1"', connection)
print(len(shabakema))
print('فراخونی محتوای shabakema')

# shabakema_1000 = shabakema[1:1000]
shabakema['platform'] = 'شبکه ما'

shabakema = shabakema[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]
# shabakema.to_excel(r'D:\python\UGC\sample\shabakema_row data_1.xlsx', index=False)

shabakema.dtypes

shabakema['content_name'].replace('', 'NO', inplace=True)
# namayesh['viwes'].replace('', 'NO', inplace=True)
shabakema = shabakema[~shabakema.content_name.str.contains("NO")]

shabakema['viwes'] = shabakema['viwes'].astype(str).astype(int)
# shabakema['follower'] = shabakema['follower'].astype(str).astype(int)
shabakema['like_count'] = shabakema['like_count'].astype(str).astype(int)

###remove dublicates on data shabakema

shabakema = shabakema.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# shabakema.to_excel(r'D:\python\UGC\sample\shabakema_r.xlsx', index=False)

print(len(shabakema))



##########################################################

#############  tamasha

tamasha = psql.read_sql('select * from public."tamasha_metatest1"', connection)
print(len(tamasha))
print('فراخونی محتوای tamasha')

# tamasha_1000 = tamasha[1:10000]
tamasha['platform'] = 'تماشا'
tamasha = tamasha[['content_name','content_link','viwes','like_count','channel_name','follower','crawling_date','categori','platform']]
# tamasha.to_excel(r'D:\python\UGC\sample\tamasha_row data_1000.xlsx', index=False)

tamasha.dtypes


tamasha['content_name'].replace('', 'NO', inplace=True)
# namayesh['viwes'].replace('', 'NO', inplace=True)
tamasha = tamasha[~tamasha.content_name.str.contains("NO")]

tamasha['viwes'] = tamasha['viwes'].astype(str).astype(int)
tamasha['follower'] = tamasha['follower'].astype(str).astype(int)
tamasha['like_count'] = tamasha['like_count'].astype(str).astype(int)

###remove dublicates on data tamasha

tamasha = tamasha.sort_values('viwes').drop_duplicates(['content_name','channel_name'], keep='last')
# tamasha.to_excel(r'D:\python\UGC\sample\tamasha_r.xlsx', index=False)

print(len(tamasha))









#############################################################

###############  merge

frames00 = [aparat_f, didestan, gabeh, mihanvideo , mp4, namasha, namayesh, shabakema, tamasha]
adgham0 = pd.concat(frames00)
# adgham0.to_excel(r'D:\python\UGC\sample\adgham000.xlsx', index=False)
print(len(adgham0))

adgham0.dtypes
# adgham0['viwes'] = adgham0['viwes'].astype(float)
# adgham0['like_count'] = adgham0['like_count'].astype(float)
# adgham0['follower'] = adgham0['follower'].astype(float)

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()

adgham0.to_sql('UGC_remove_dublicate',con,if_exists='replace', index=False)

print('اتمام برنامه')

adgham0.dtypes

learn_ugc1 = adgham0.query("categori == 'آموزشی'")
learn_ugc2 = adgham0.query("categori == 'آموزش موسیقی'")



learn_ugc0 = [learn_ugc1, learn_ugc2]
learn_ugc = pd.concat(learn_ugc0)

# learn_ugc['like_count'] = learn_ugc['like_count'].astype(int)

print('learn :',len(learn_ugc))
learn_ugc.to_excel(r'D:\python\PowerBI_App_store\progress\test\learn_ugc.xlsx', index=False)
learn_ugc.to_sql('learn_ugc',con,if_exists='replace', index=False)




adgham0['categori'] = adgham0['categori'].str.replace('بازی\u200c', 'بازی')

game_ugc1 = adgham0.query("categori == 'بازی'")
game_ugc2 = adgham0.query("categori == 'گیم'")


game_ugc0 = [game_ugc1, game_ugc2]
game_ugc = pd.concat(game_ugc0)



game_ugc = game_ugc.query("platform != 'جعبه'")



print('learn :',len(game_ugc))
game_ugc.to_excel(r'D:\python\PowerBI_App_store\progress\test\game_ugc1.xlsx', index=False)
game_ugc.to_sql('game_ugc',con,if_exists='replace', index=False)




