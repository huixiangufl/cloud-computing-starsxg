#!/usr/bin/env python
import sys
import numpy as np
"""
This function randomly generate a random walk matrix,
and writes the result in a format specified by pagerank_mapped.py
"""

def normalise_by_row(mat):
    return mat / np.sum(mat,1).reshape((len(mat),1))

def generate_random_walk_mat(size, file_name):
    random_prob_matrix = np.random.random((size,size))
    random_transit_matrix = normalise_by_row(random_prob_matrix)
    print random_transit_matrix
    random_transit_matrix = random_transit_matrix.transpose()
    print random_transit_matrix
    fh = open(file_name,'w')
    for i in range(size):
        fh.write(("# %d" % i))
        for item in random_transit_matrix[i]:
            fh.write((" %f" % item))
        fh.write("\n")
    fh.close()
        
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage prog mat_size output_file"   
        exit(1)
    mat_size = int(sys.argv[1])
    file_name = sys.argv[2]
    generate_random_walk_mat(mat_size, file_name)
