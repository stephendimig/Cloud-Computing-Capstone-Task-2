#### GROUP 2_3 #####

from __future__ import print_function
import sys
import uuid
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
        print("Usage: carrierandorigdest.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonCarrierAndAirport")
    ssc = StreamingContext(sc, 1)
    
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: ((str(line.split(',')[3] + ":" + line.split(',')[4]), line.split(',')[6]), (0 if False == line.split(',')[8].isdigit() else int(line.split(',')[8]), 1))) \
               .reduceByKey(lambda x,y: ((x[0] + y[0], x[1] + y[1])))
        

    def process(time, rdd):
        proc_data = rdd.map(lambda p: ((p[0][0], p[0][1], p[1][0], p[1][1])))
        if(False == proc_data.isEmpty()):
            proc_data.foreach(rdd2csv)

    def rdd2csv(p):
        origin = p[0].split(':')[0]
        dest = p[0].split(':')[1]
        carrier = p[1]
        arrdelay = p[2]
        total = p[3]

        query = "UPDATE mykeyspace.results2_3 SET arrdelay=arrdelay + " + str(arrdelay) + ", total=total + " + str(total) + " WHERE origin='" + origin + "' AND dest='" + dest + "' AND carrier='" + carrier + "';"
        getSession().execute(query)

    data.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()


