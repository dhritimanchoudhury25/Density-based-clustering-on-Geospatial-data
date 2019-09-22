import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import DBSCAN
from collections import Counter
from pyhive import presto


##establishing presto connection
cons=presto.connect(host='host',
port=port,
schema='schema',
username='username')
##import table from hive table having retailer lar long data
sql="select * from table"
cursor=cons.cursor()
cursor.execute(sql)
##converting the data to a pandas dataframe
ret=pd.DataFrame(cursor.fetchall())
##renaming the columns as it gives columns name as '0','1','2'
ret.rename(columns={0: "user_id", 1: "latitude", 2:"longitude"},inplace=True)
##drop the rows having nulls
ret = ret.dropna(how='any',axis=0)


##defining the kms per radian
kms_per_rad = 6371.0088
##converting kilometers to radian 
##epsilon taken as 100m as the radius of each cluster
epsilon=0.1/kms_per_rad
##create a list of unique retailers from the data frame
retailers={}
retailers=ret.retailer_id.unique()

##function to compute the coordinates of centroid of each cluster
def get_centroid(cohort):
   cluster_ary=np.asarray(cohort)
   centroid=cluster_ary.mean(axis=0)
   return centroid

##function to compute the cluster that has the post points falling inside for each user
def most_frequent(List):
   occurence_count = Counter(List)
   return occurence_count.most_common(1)[0][0]

##function to apply DBscan algorithm and computing the centroid coordinates for each user
def apply_dbscan(cluster):
   dbsc = (DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine')
       .fit(np.radians(cluster)))
   ret_cluster_labels = dbsc.labels_
   num_clusters = len(set(dbsc.labels_))
   dbsc_clusters = pd.Series([cluster[ret_cluster_labels==n] for n in range(num_clusters)])
   ret_centroids = dbsc_clusters.map(get_centroid)
   cent=most_frequent(ret_cluster_labels)
   ret_centroid=ret_centroids.iloc[int(cent)]
   return ret_centroid

##create a dataframe of retailers with their coordinates
columns=['latitude','longitude','smart_id']
ret_loc=pd.DataFrame()

ret_list = []

for retailer_id in retailers:

    cohort=ret[(ret.retailer_id ==retailer_id)]
    cohort=cohort.iloc[:,1:3]
    ret_centroid=apply_dbscan(cohort)
    retailer_location=np.append(ret_centroid,str(retailer_id))
    ret_list.append(retailer_location)


ret_loc=pd.DataFrame(ret_list, columns=columns)
print(ret_loc.head())

##saving the data to local in csv format
ret_loc.to_csv(r'location.csv', index=False)