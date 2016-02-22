from __future__ import print_function

import sys
import uuid
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import uuid

def getSession():
    if ('session' not in globals()):
        cluster = Cluster()
        globals()['session'] = cluster.connect()
        globals()['session'].default_consistency_level = 1
    return globals()['session']

def flatten(l):
    retval = False
    for elem in l:
        if(True == elem):
            retval = True
    return retval

def digit(p):
    return 0 if False == p.isdigit() else int(p)

def process(time, rdd):
    if(False == rdd.isEmpty()):
        rdd.foreach(rdd2csv)
    

#flightno, origin, dest, carrier, date, dep_time, arrival_delay
def rdd2csv(p):
    flightno = p[0]
    origin = p[1]
    dest = p[2]
    carrier = p[3]
    date = p[4]
    dep_time = p[5]
    arrival_delay = p[6]
    
    id = uuid.uuid4()

    query = "UPDATE mykeyspace.results3_2 SET flightno=" + str(flightno) + ", origin='" + origin + "', dest='" + dest + "', carrier='" + carrier + "', date='" + date + "', dep_time=" + str(dep_time) + ", arrival_delay=" + str(arrival_delay) + " WHERE id='" + str(id) + "';"
    getSession().execute(query)

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

    zkQuorum, topic = sys.argv[1:3]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    #flightno, origin, dest, carrier, date, dep_time, arrival_delay
    lines = kvs.map(lambda x: x[1])
    data = lines.map(lambda line: ((digit(line.split(',')[2]), line.split(',')[3], line.split(',')[4], line.split(',')[6], line.split(',')[1], digit(line.split(',')[10]), digit(line.split(',')[8])))) \
        .filter(lambda p: (flatten([(p[1] == xvals[index] and p[2] == yvals[index] and p[5] < 1200) or (p[1] == yvals[index] and p[2] == zvals[index] and p[5] > 1200) for index in range(0, len(xvals))])))
    
    data.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


