#!/usr/bin/python

import os
import time
import re
import subprocess
from subprocess import Popen, PIPE, STDOUT

cmdpipe = subprocess.Popen("/usr/bin/cqlsh -k mykeyspace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=PIPE, shell=True, bufsize=0)


while(1):
    dirlist = []
    llist = subprocess.Popen(["hadoop", "fs", "-ls", "/user/root/output/"], stdout=subprocess.PIPE)
    for line in llist.stdout:
        sline = line.split(' ')
        dir = sline[-1][:-1]
        if(re.search('.*group2_3.*$', dir)):
            dirlist.append(dir)

    dirlist = sorted(dirlist, key=lambda x: int(x.split('.')[1])) 
    total = len(dirlist)
    last = 0
    for dir in dirlist:
        if(total <= 2):
            print("Sleeping...")
            time.sleep(5)

        print("Processing: " + dir)
        cmd = "hadoop fs -cat " + dir + "/*part* > /root/group2_3.csv"
        os.system(cmd)

        if(total == len(dirlist) or 1 == total or (last - total) >= 4):
            last = total
            print("Importing ...")
            stmnt = "truncate results2_3;\n"
            cmdpipe.stdin.write(stmnt)
            stmnt = "COPY results2_3 (origin, dest, delayavg, carrier) FROM '/root/group2_3.csv';\n"
            cmdpipe.stdin.write(stmnt)

        print("Removing: " + dir)
        cmd = "hadoop fs -rm -r -f " + dir
        os.system(cmd)

        print("Removing: " + "/root/group2_3.csv")
        cmd = "rm -f /root/group2_3.csv"

        total = total - 1

    print("Sleeping...")
    time.sleep(10)
