"""
@Author : Mohit Jain
@RollNo : 201202164
"""

import argparse
import random

def createRandomTuples(n_tups, n_attr, len_attr):
    '''
    Returns a list of n_tups random tuples in csv format in the range of [len_attr,len_attr*10)
    '''
    return  [','.join([str(x) for x in random.sample(xrange(len_attr,len_attr*10), n_attr)]) for i in range(n_tups)] 

def createRelationFile(f_path, f_size, n_attr, block_size):
    '''
    Creates a relations csv-tuple file using randmon numbers'
    '''
    # Create 100 really random tuples.
    n_tups = 100
    len_attr = 10000
    randTuples = createRandomTuples(n_tups, n_attr, len_attr)

    cur_f_size = 0
    with open(f_path, 'w') as f:
        while cur_f_size < f_size:
            # Sample from the n_tups random tuples created.
            tupleToWrite = '\n'.join(random.sample(randTuples, n_tups))+'\n'
            f.write(tupleToWrite)
            cur_f_size += n_tups*n_attr*len(str(len_attr))

def getArguments():
    '''
    Loads the command line arguments and returns a dictionary of the arguments.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-file1',dest='file1', type=str)
    parser.add_argument('-file1size',dest='s_file1', type=int)
    parser.add_argument('-file2',dest='file2', type=str)
    parser.add_argument('-file2size',dest='s_file2', type=int)
    parser.add_argument('-n',dest='n_attr', type=int)
    parser.add_argument('-M',dest='n_blocks', type=int)
    parser.add_argument('-type',dest='index_type', type=str)

    args = parser.parse_args()
    return args

def main():
    '''
    Loader function.
    '''
    # Global Variable Definitions
    block_size = 100000 #1e+5

    # Load command line arguments
    args = getArguments()
    n_blocks = args.n_blocks
    n_attr = args.n_attr
    
    # Print state
    print "[INFO] Global Variable State :"
    print "Block size  :",block_size
    print "#Blocks     :",n_blocks
    print "#Attributes :",n_attr

    # Create Relation Files
    file1_path = args.file1
    file1_size = args.s_file1
    print "[INFO] Creating random tuples for relation1 at", file1_path
    createRelationFile(file1_path, file1_size, n_attr, block_size)

    file2_path = args.file2
    file2_size = args.s_file2
    print "[INFO] Creating random tuples for relation2 at", file2_path
    createRelationFile(file2_path, file2_size, n_attr, block_size)

    
    return

if __name__=='__main__':
    main()
