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

def getUniqueNum():
    if ('uniqueNum' not in globals()):
        globals()['uniqueNum'] = 1
    else:
         globals()['uniqueNum'] = globals()['uniqueNum'] + 1
    return globals()['uniqueNum']


def flatten(l):
    retval = False
    for elem in l:
        if(True == elem):
            retval = True
    return retval

def digit(p):
    return 0 if False == p.isdigit() else int(p)

def updateFunc(new_values, last_sum):
    return last_sum


def process(time, rdd):
    proc_data = rdd.map(lambda p: (p[1]))
    proc_data.foreach(lambda p: (print("P=" + p)))
    if(False == proc_data.isEmpty()):
        uri = "hdfs://sandbox.hortonworks.com/user/root/output/group3_2." + str(getInvokeNum())
        proc_data.saveAsTextFile(uri)
    

#flightno, origin, dest, carrier, date, dep_time, arrival_delay
def rdd2csv(p):
    unique_id = p[0]
    flightno = p[1]
    origin = p[2]
    dest = p[3]
    carrier = p[4]
    date = p[5]
    dep_time = p[6]
    arrival_delay = p[6]
    return str(unique_id) + "," + str(flightno) + "," + origin + "," + dest +  "," + carrier + "," + date + "," + str(dep_time) + "," + str(arrival_delay)

if __name__ == "__main__":
    xvals = []
    yvals = []
    zvals = []
    if len(sys.argv) < 3:
        print("Usage: tomstrip.py <zk> <topic> <XXX:YYY:ZZZ>", file=sys.stderr)
        exit(-1)
    elif(len(sys.argv) > 3):
        for index in range(3,len(sys.argv)):
            xvals.append(sys.argv[index].split(':')[0])
            yvals.append(sys.argv[index].split(':')[1])
            zvals.append(sys.argv[index].split(':')[2])

    sc = SparkContext(appName="PythonTomsTrip")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("storage")
    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(-1, "")])


    zkQuorum, topic = sys.argv[1:3]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    #flightno, origin, dest, carrier, date, dep_time, arrival_delay
    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: ((getUniqueNum(), digit(line.split(',')[2]), line.split(',')[3], line.split(',')[4], line.split(',')[6], line.split(',')[1], digit(line.split(',')[10]), digit(line.split(',')[8]))))

    my_data = data.filter(lambda p: (flatten([(p[2] == xvals[index] and p[3] == yvals[index] and p[6] < 1200) or (p[2] == yvals[index] and p[3] == zvals[index] and p[6] > 1200) for index in range(0, len(xvals))])))
    
    
    my_data = my_data.map(lambda p: ((p[0], rdd2csv(p))))
    #my_data = my_data.updateStateByKey(updateFunc)
    my_data.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


