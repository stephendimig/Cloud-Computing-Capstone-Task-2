from __future__ import print_function

import sys
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: topairports.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonTopAirports")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("storage")

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(u'airport', -1)])
    
    lines = kvs.map(lambda x: x[1])
    csv_data = lines.map(lambda line: (line.split(',')[3] + "," + line.split(',')[4])) \
	.flatMap(lambda x: x.split(',')) \
	.map(lambda x: (x, 1)) \
	.updateStateByKey(updateFunc)
    
    
    def process(time, rdd):  
        sqlContext = getSqlContextInstance(rdd.context)
        
        row_data = rdd.map(lambda p: Row(airport=p[0], total=int(p[1])))
        df = sqlContext.createDataFrame(row_data)
        df.registerTempTable("ontime")
        handle = sqlContext.sql(""" SELECT * FROM ontime ORDER BY total DESC LIMIT 10 """)
        handle.show()

    csv_data.foreachRDD(process) 
    ssc.start()
    ssc.awaitTermination()
