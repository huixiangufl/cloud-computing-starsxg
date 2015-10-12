###########################################################################
#
# Copyright (c) 2015 Adobe Systems Incorporated. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
###########################################################################


# This file implements GPU Sort using Spark's mapPartition. 

import sys, getopt
import numpy as np
import threading
import time

from pyspark import SparkContext

import numbapro.cudalib.sorting as sorting # Numbapro need to be installed


def parseVector(line):
    return eval(line)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sort <file>"
        exit(-1)
    #start = time.time()
    sc = SparkContext(appName="PythonSort")
    rdd = sc.textFile(sys.argv[1])
    data = rdd.map(parseVector).cache()
    data.count() # force action before sorting 

    start = time.time()
    sorted_rdd = data.gpuSortByKey()
    sorted_rdd.count() # Force action
#    for x in sorted_rdd.collect(): #Huixiang Chen
#        print x
    sc.stop()

    stop = time.time()
    print "execution time: ", stop-start
