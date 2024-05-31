from kafka import KafkaConsumer
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
import pymongo
from pyspark.sql.functions import col


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["products"]
collection = db["suggestions"]

spark = SparkSession.builder \
    .appName("ALS") \
    .getOrCreate()








model = ALSModel.load(r'50_set')
asin = spark.read.csv('unique_asin.csv',header=True)
consumer = KafkaConsumer('spark', bootstrap_servers=['localhost:9092'])


print('ready')


for message in consumer:
	values = eval(message.value.decode('utf-8'))
	bought = values['bought'].split(',')
	

	try:
		uid = values['new']
		uid = int(uid)
		test = spark.createDataFrame([(uid,)], ["reviewerID_index"])
		recommendations = model.recommendForUserSubset(test, 5)
		data = recommendations.collect()[0][1]
		items = []
		i_str = []
		
		for i in data:
			val = i.asin_index
			items.append(val)
			i_str.append(str(val))
		
		data = asin.filter(col('index').isin(i_str)).select('asin').collect()
		items = []
		for i in data:
			items.append(i.asin)
	except:
		items = ['038568231X','0007420412','0297859382','0141353678','0312577222']

	doc = {'reviewer_id':values['R_id'],'products':items,'bought':bought}
	collection.insert_one(doc)
