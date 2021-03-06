#### GROUP 2_1 #####

from __future__ import print_function
import sys
import uuid
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

def getSession():
    if ('session' not in globals()):
        cluster = Cluster()
        globals()['session'] = cluster.connect()
        globals()['session'].default_consistency_level = 1
    return globals()['session']

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: carrierandairport.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonCarrierAndAirport")
    ssc = StreamingContext(sc, 1)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: ((line.split(',')[3].strip(), line.split(',')[6].strip()), (0 if False == line.split(',')[11].isdigit() else int(line.split(',')[11]), 1))) \
               .reduceByKey(lambda x,y: ((x[0] + y[0], x[1] + y[1])))
        
    def process(time, rdd):
        proc_data = rdd.map(lambda p: ((p[0][0], p[0][1], p[1][0], p[1][1])))
        if(False == proc_data.isEmpty()):
            proc_data.foreach(rdd2csv)

    def rdd2csv(p):
        origin = p[0]
        carrier = p[1]
        depdelay = p[2]
        total = p[3]
        if(origin != "Origin"):
            query = "UPDATE mykeyspace.results2_1 SET depdelay=depdelay + " + str(depdelay) + ", total=total + " + str(total) + " WHERE origin='" + origin + "' AND carrier='" + carrier + "';"
            getSession().execute(query)

    data.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
