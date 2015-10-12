
import sys, getopt
import numpy as np
import threading
import time
import gc

from random import random
from operator import add

from pyspark import SparkContext
from pycuda import gpuarray, reduction
import pycuda.driver as cuda

class GPUPi(threading.Thread):
    def __init__(self, dev_id, rdd):
        self.dev_id = dev_id
        self.rdd = rdd


    def gpuCalculatePi(self):
      def gpuPi(iterator):
        iterator = iter(iterator)
        length = len(list(iterator))
        a = np.random.random_sample(length)
        b = np.random.random_sample(length)

        cuda.init()
        dev = cuda.Device(0)
        contx = dev.make_context()


        gpu_a = gpuarray.to_gpu(a)
        gpu_b = gpuarray.to_gpu(b)
        countkrnl = reduction.ReductionKernel(np.float64, neutral = "0",
                                              map_expr = "(a[i]*a[i] + b[i]*b[i] >= 1.0) ? 1.0 : 0.0",
                                              reduce_expr = "a+b", arguments = "float * a, float * b")
        pointInsideCircle = countkrnl(gpu_a, gpu_b).get()
        yield pointInsideCircle

        contx.pop()
        del gpu_a
        del gpu_b
        gc.collect()

      pointsInsideCircle = self.rdd.mapPartitions(gpuPi)
      return pointsInsideCircle



if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    start = time.time()
    sc = SparkContext(appName="PythonPi")
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    n = 100000 * partitions

    inputList = sc.parallelize(range(1, n+1), partitions)
    gpuPi = GPUPi(0, inputList)
    pointsInsideCircle = gpuPi.gpuCalculatePi()
    allPointsInsideCircle = pointsInsideCircle.reduce(add)
    print("Pi is roughly %f" % (4.0 * allPointsInsideCircle / n))
    sc.stop()
    stop = time.time()
    print "execution time: ", stop-start, "seconds"
