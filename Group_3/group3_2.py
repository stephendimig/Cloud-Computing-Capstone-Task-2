#!/usr/bin/python

import os
import time
import re
import subprocess
from subprocess import Popen, PIPE, STDOUT

cmdpipe = subprocess.Popen("/usr/bin/cqlsh -k mykeyspace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=PIPE, shell=True, bufsize=0)

stmnt = "truncate results3_2;\n"
cmdpipe.stdin.write(stmnt)

while(1):
    dirlist = []
    llist = subprocess.Popen(["hadoop", "fs", "-ls", "/user/root/output/"], stdout=subprocess.PIPE)
    for line in llist.stdout:
        sline = line.split(' ')
        dir = sline[-1][:-1]
        if(re.search('.*group3_2.*$', dir)):
            dirlist.append(dir)

    dirlist = sorted(dirlist, key=lambda x: int(x.split('.')[1])) 
    for dir in dirlist:
        print("Processing: " + dir)
        cmd = "hadoop fs -cat " + dir + "/*part* > /root/group3_2.csv"
        os.system(cmd)

        print("Importing ...")
        stmnt = "COPY results3_2 (id, flightno, origin, dest, carrier, date, dep_time, arrival_delay) FROM '/root/group3_2.csv';\n"     
        cmdpipe.stdin.write(stmnt)

        print("Removing: " + dir)
        cmd = "hadoop fs -rm -r -f " + dir
        os.system(cmd)

        print("Removing: " + "/root/group3_2.csv")
        cmd = "rm -f /root/group3_2.csv"

    print("Sleeping...")
    time.sleep(10)
