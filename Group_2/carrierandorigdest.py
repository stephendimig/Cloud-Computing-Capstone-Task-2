from __future__ import print_function

import sys
import uuid
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def getInvokeNum():
    if ('invokeNum' not in globals()):
        globals()['invokeNum'] = 1
    else:
         globals()['invokeNum'] = globals()['invokeNum'] + 1
    return globals()['invokeNum']


def updateFunc(new_values, last_sum):
    retval = (last_sum or (0, 0))
    for (x, y) in new_values:
        retval = (x + retval[0], y + retval[1])
    return retval

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: carrierandorigdest.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonCarrierAndAirport")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("storage")
    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([((u'origin:dest', u'carrier'), (2147483647, 1))])
    
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: ((str(line.split(',')[3] + ":" + line.split(',')[4]), line.split(',')[6]), (0 if False == line.split(',')[8].isdigit() else int(line.split(',')[8]), 1))) \
               .updateStateByKey(updateFunc)
        

    def process(time, rdd):
        uri = "hdfs://sandbox.hortonworks.com/user/root/output/group2_3." + str(getInvokeNum())
        proc_data = rdd.map(lambda p: ((p[0][0], p[0][1], p[1][0], p[1][1]))) \
                    .map(lambda p: (float(float(p[2])/float(p[3])), (p[0], p[1]))) \
                    .sortByKey() \
                    .map(lambda p: ((p[1][0], (p[1][1], p[0])))) \
                    .groupByKey() \
                    .map(lambda p : (p[0], list(p[1])))
        proc_data = proc_data.map(rdd2csv).flatMap(lambda x: (x))
        if(False == proc_data.isEmpty()):
            proc_data.saveAsTextFile(uri)

    def rdd2csv(p):
        origin = p[0].split(':')[0]
        dest = p[0].split(':')[1]
        mlist = p[1]
        count = 0
        rlist = []
        for mtuple in mlist:
            if(count < 10):
                count = count + 1
            else :
                break
            rlist.append(str(origin + "," + dest +  "," + str(mtuple[1]) + "," + mtuple[0]))
        return rlist
          
    csv_data = data.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


