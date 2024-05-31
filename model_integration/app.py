from flask import Flask, render_template, request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaProducer
import json
from time import sleep
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["products"]
collection = db["suggestions"]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                           	value_serializer=lambda m: json.dumps(m).encode('utf-8'))





def fetch(x):
    sleep(10)
    latest_document = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
    while latest_document['_id'] == x:
        latest_document = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
    
    return latest_document


spark = SparkSession.builder \
    .appName("FLASK") \
    .getOrCreate()


ids = spark.read.csv('unique_reviewer.csv',header=True)
app = Flask(__name__)

@app.route('/', methods=['GET'])
def start():
    return render_template('index.html')

@app.route('/results',methods=['POST'])
def link():
    if request.method == 'POST':           
        requests = request.form.to_dict()  
        id = [requests['R_id']]
        
        index = ids.filter(col('reviewerID').isin(id)).select('index').collect()
        
        try:
            requests['new'] = index[0]['index']
            producer.send('spark',requests)
        except:
            producer.send('spark',requests)
        
        start = collection.find_one(sort=[("_id", pymongo.DESCENDING)])['_id']
        x = fetch(start)
        x = x['products']
    
    return render_template('result.html',products = x)

        

if __name__ == '__main__':
    app.run(port=3000,debug=True)

