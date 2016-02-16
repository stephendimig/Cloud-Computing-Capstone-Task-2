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
    retval = (last_sum or (0, 0))
    for (x, y) in new_values:
        retval = (x + retval[0], y + retval[1])
    return retval

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: topcarriers.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonTopCarriers")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("storage")

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(u'carrier', (2147483647, 1))])

    lines = kvs.map(lambda x: x[1])
    csv_data = lines.map(lambda line: (line.split(',')[6], (0 if False == line.split(',')[8].isdigit() else int(line.split(',')[8]), 1))) \
               .updateStateByKey(updateFunc)

    def process(time, rdd):  
        sqlContext = getSqlContextInstance(rdd.context)

        row_data = rdd.map(lambda p: Row(carrier=p[0], arrivaldelay=int(p[1][0]), total=int(p[1][1]), delayavg=float(float(p[1][0])/float(p[1][1]))))
        df = sqlContext.createDataFrame(row_data)
        df.registerTempTable("ontime")
        handle = sqlContext.sql(""" SELECT carrier, delayavg  FROM ontime WHERE carrier != 'Carrier' ORDER BY delayavg ASC LIMIT 10 """)
        handle.show()

    csv_data.foreachRDD(process) 
   
    ssc.start()
    ssc.awaitTermination()
