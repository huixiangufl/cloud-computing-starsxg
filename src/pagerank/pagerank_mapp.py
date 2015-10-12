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


# This file implements GPU Word Count using Spark's mapPartition. 

import sys, getopt
import numpy as np
import threading
import time
import gc

from pyspark import SparkContext

from pycuda import gpuarray, reduction
import pycuda.driver as cuda

from operator import add

"""
The input file format

# nodeID prob0 prob1 prob2 ...

prob# is the transition probability
"""

class GPUWCThread(threading.Thread):
    def __init__(self, dev_id, rdd):
        threading.Thread.__init__(self)
        self.dev_id = dev_id
	self.rdd = rdd

    def gpuWordCount(self,PVector):
	def gpuFunc(iterator):
	    # 1. Data preparation
            iterator = iter(iterator)
            cpu_data = list(iterator)
            """
            #print cpu_data
            cpu_dataset = " ".join(cpu_data)
            #print cpu_dataset
            ascii_data = np.asarray([ord(x) for x in cpu_dataset], dtype=np.uint8)
            #print ascii_data
	    # 2. Driver initialization and data transfer
	    cuda.init()
	    dev = cuda.Device(0)
	    contx = dev.make_context()
            gpu_dataset = gpuarray.to_gpu(ascii_data)

	    # 3. GPU kernel.
	    # The kernel's algorithm counts the words by keeping 
	    # track of the space between them
            countkrnl = reduction.ReductionKernel(long, neutral = "0",
            		map_expr = "(a[i] == 32)*(b[i] != 32)",
                        reduce_expr = "a + b", arguments = "char *a, char *b")

            results = countkrnl(gpu_dataset[:-1],gpu_dataset[1:]).get()
            #print results
            value.append(3)
            print "value " + str(value)
            yield results , [1,2,3]
            """
            print "PVector", PVector
            dpFactor = 0.85
            pSum = sum(PVector)
            numLines = len(cpu_data)
            
            pList = []
            cuda.init()
            dev = cuda.Device(0)
            contx = dev.make_context()

            for i in range(numLines):
                if cpu_data[i][0] != '#':
                    continue
                firstSpaceIndex = cpu_data[i].find(' ')
                secondSpaceIndex = cpu_data[i].find(' ', firstSpaceIndex+1)
                nodeID = int(cpu_data[i][firstSpaceIndex+1:secondSpaceIndex])
                probListStr = cpu_data[i][secondSpaceIndex+1:].split(' ')
                probListFlt = map(float,probListStr)              
                matSize = len(probListStr)
                #probListFlt = [float(x)*dpFactor + (1-dpFactor) for x in probListStr]
                probListFlt = [x*dpFactor + (1-dpFactor)/matSize for x in probListFlt]
                npProbListFlt = np.asarray(probListFlt,dtype = np.float32)
                npPVector = np.asarray(PVector,np.float32)
                #cuda.init()
                #dev = cuda.Device(0) 
                #contx = dev.make_context()
                gpu_matVect = gpuarray.to_gpu(npProbListFlt)               
                gpu_pVect = gpuarray.to_gpu(npPVector)
                countkrnl = reduction.ReductionKernel(np.float32, neutral = "0", 
                                                      map_expr = "a[i]*b[i]", reduce_expr = "a+b", arguments = "float *a, float *b")
                result = countkrnl(gpu_matVect, gpu_pVect).get()
                if i == 0:
                    pList = [0.0] * matSize
                pList[nodeID] = result
            yield pList  
	    # Release GPU context resources
	    contx.pop() 
	    del gpu_matVect
            del gpu_pVect
            del contx
	   
	    gc.collect()            
		    	
    	vals = self.rdd.mapPartitions(gpuFunc)
	return vals

def toascii(data):
    strs = " ".join(data)
    return np.asarray([ord(x) for x in data], dtype=np.uint8)

if __name__ == "__main__":
    if len(sys.argv) != 3:
  	print >> sys.stderr, "Usage: ./spark-submit gpuwc <file> mat_size"
        exit(-1)
    start = time.time()
    sc = SparkContext(appName = "wordCount")    
    Rdd = sc.textFile(sys.argv[1])
    gpuwc_thread = GPUWCThread(0,Rdd)
    mat_size = int(sys.argv[2])
    pVector = [1.0] * mat_size
    numIteration = 5
    for i in range(numIteration):
        print "iteration",i
        partial_count_rdd = gpuwc_thread.gpuWordCount(pVector)
        total  = partial_count_rdd.collect()
        pVector = partial_count_rdd.reduce(lambda x,y:map(add,x,y))
    print "pList",pVector
    sc.stop
    stop = time.time()
    #Just a convenient way to measure time in local mode 
    print "execution time: ", stop-start, "seconds"
