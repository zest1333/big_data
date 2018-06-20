import pandas as pd
import numpy as np

pd.set_option('max_colwidth',1000)
pd.set_option('max_columns',1000)

business = sqlContext.read.option("inferSchema", "true").json("s3://yelp-com/business/business.json")

# State, City 별 Aggregation
gr = df_b.groupby(['state','city'])
df_r = gr.agg({'business_id':'count','stars':'mean'})
pby(['state','city']).agg({'business_id':'count','stars':'mean'})], axis=1, sort=False)
result.columns = ['total_count','total_stars_mean','star4_count','star4_mean']
result.fillna(0,inplace=True)


# 아시안 카테고리 별 Aggregation
cat_asian = ['Japanese','Chinese','Thai','Asian Fusion','Sushi Bars','Ramen','Dim Sum']
df_ba = df_b[df_b['categories'].map(lambda x:np.sum([c in x for c in cat_asian])>0)]
df_ba_agg = df_ba.groupby(['state','city']).agg({'business_id':'count','stars':'mean'})
result = pd.merge(result,df_ba_agg, left_index=True, right_index=True, how='outer',indicator=True)
result.columns = ['total_count','total_stars_mean','star4_count','star4_mean','star4_ratio','asian_count','asian_star_mean','asian_merge']


# Korean 카테고리 별 Aggregation
cat_korean = ['Korean']
df_bk = df_b[df_b['categories'].map(lambda x:np.sum([c in x for c in cat_korean])>0)]
df_bk_agg = df_bk.groupby(['state','city']).agg({'business_id':'count','stars':'mean'})
result = pd.merge(result,df_bk_agg, left_index=True, right_index=True, how='outer',indicator=True)
result.columns = ['total_count','total_stars_mean','star4_count','star4_mean','star4_ratio','asian_count','asian_star_mean','asian_merge','korean_count','korean_star_mean','korean_merge']


# 종합
result['asian_count'].fillna(0,inplace=True)
result['asian_star_mean'].fillna(0,inplace=True)
result['korean_count'].fillna(0,inplace=True)
result['korean_star_mean'].fillna(0,inplace=True)

result['korean_per_asian'] = result['korean_count']/result['asian_count']
result['korean_per_asian'].fillna(0,inplace=True)


result['asian_ratio'] = result['asian_count'] / result['total_count']
result['korean_ratio'] = result['korean_count'] / result['total_count']

result['asian_ratio'].fillna(0,inplace=True)
result['korean_ratio'].fillna(0, inplace=True)


result.reset_index(inplace=True)
dfr = sqlContext.createDataFrame(result)

dfr.createTempView('result')


# 상위 10위 State
%sql
select state, sum(total_count) as total , sum(asian_count) as a, sum(korean_count) as k
from result4
group by state
order by a desc, k asc
limit 10



