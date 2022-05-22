# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 11:30:52 2021

@author: vahid

for UGC analyese

"""
## input data

import pandas as pd
import numpy as np
import datetime
import os
import time

start = time.time() 


df0 = pd.DataFrame()
a = pd.DataFrame()
b = pd.DataFrame()
dfff1 = pd.DataFrame()
Bazar_1 = pd.DataFrame()
total = pd.DataFrame()

##### input data
df_aparat_1 = pd.read_csv(r'D:\python\UGC\aparat\meta_data\aparat_meta2.csv')
df_aparat_2 = pd.read_csv(r'D:\python\UGC\aparat\meta_data\aparat_meta3.csv')
df_aparat_3 = pd.read_csv(r'D:\python\UGC\aparat\meta_data\aparat_meta4.csv')
df_aparat_4 = pd.read_csv(r'D:\python\UGC\aparat\meta_data\aparat_meta5.csv')

df_Tamasha_1 = pd.read_csv(r'D:\python\UGC\Tamasha\tamasha_meta1.csv')
df_Tamasha_2 = pd.read_csv(r'D:\python\UGC\Tamasha\tamasha_meta2.csv')
df_Tamasha_3 = pd.read_csv(r'D:\python\UGC\Tamasha\tamasha_meta3.csv')
df_Tamasha_4 = pd.read_csv(r'D:\python\UGC\Tamasha\tamasha_meta4.csv')

df_aparat_1 = df_aparat_1.append(df_aparat_2)
df_aparat_3 = df_aparat_3.append(df_aparat_1)
df_aparat_4 = df_aparat_4.append(df_aparat_3)
# left = df['Identifier'].str[:5]
df_aparat_4['m_crwaling_date'] = df_aparat_4['crwaling_date'].str[:10]
df_aparat_4['viwes'] = df_aparat_4['viwes'].str.replace('بازدید','')
df_aparat_4['viwes'].str.strip()
df_aparat_4['viwes'].str.strip()
# df_aparat_4['viwes'].str.strip()
# df_aparat_4['viwes'] = df_aparat_4['viwes'].astype(int)
df_aparat_4['viwes'] = df_aparat_4['viwes'].astype(str).astype(int)
df_aparat_4['follower'] = df_aparat_4['follower'].str.replace('دنبال کننده','')
df_aparat_4['follower'] = df_aparat_4['follower'].str.replace('هزار','k')
# df_aparat_4['follower_1'] = df_aparat_4['follower'].str.replace('هزار','1000')
df_aparat_4['follower'].str.strip()
# df_aparat_4['follower_1'].str.strip()

# df_aparat_4['follower'] = df_aparat_4['follower'].astype(str).astype(int)
# df_aparat_4['follower_1'] = df_aparat_4['follower_1'].astype(str).astype(int)


# df_aparat_4['follower_1'] = df_aparat_4.loc[df_aparat_4['follower'].str.contains("هزار", case=False)]
# df_aparat_4.loc[df_aparat_4['follower'].str.contains('هزار'), 'follower'] = '1000'

df_aparat_sport=df_aparat_4[df_aparat_4['page_link'].str.contains('sport', regex=False)]
df_aparat_sport['channel']='sport'

df_aparat_animated=df_aparat_4[df_aparat_4['page_link'].str.contains('animated', regex=False)]
df_aparat_animated['channel']='animated'

df_aparat_art=df_aparat_4[df_aparat_4['page_link'].str.contains('art', regex=False)]
df_aparat_art['channel']='art'

df_aparat_entertainment=df_aparat_4[df_aparat_4['page_link'].str.contains('entertainment', regex=False)]
df_aparat_entertainment['channel']='entertainment'

df_aparat_news=df_aparat_4[df_aparat_4['page_link'].str.contains('news', regex=False)]
df_aparat_news['channel']='news'

df_aparat_health=df_aparat_4[df_aparat_4['page_link'].str.contains('health', regex=False)]
df_aparat_health['channel']='health'

df_aparat_political=df_aparat_4[df_aparat_4['page_link'].str.contains('political', regex=False)]
df_aparat_political['channel']='political'

df_aparat_religious=df_aparat_4[df_aparat_4['page_link'].str.contains('religious', regex=False)]
df_aparat_religious['channel']='religious'

df_aparat_women=df_aparat_4[df_aparat_4['page_link'].str.contains('women', regex=False)]
df_aparat_women['channel']='women'

# df_aparat_=df_aparat_4[df_aparat_4['page_link'].str.contains('', regex=False)]
# df_aparat_['channel']=''



frames = [df_aparat_sport, df_aparat_animated, df_aparat_art, df_aparat_entertainment,df_aparat_news,df_aparat_health,df_aparat_political,df_aparat_religious,df_aparat_women]
result = pd.concat(frames)

 
result.to_excel(r'D:\python\UGC\aparat\meta_data\aparat1.xlsx',index=False)



