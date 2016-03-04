"""
@Author : Mohit Jain
@RollNo : 201202164
"""

import sys
import argparse
import random

def createRandomTuples(n_tups, n_attr, len_attr):
    '''
    Returns a list of n_tups random tuples in csv format in the range of [len_attr,len_attr*10)
    '''
    return  [','.join([str(x) for x in random.sample(xrange(pow(10,len_attr),pow(10,len_attr+1)), n_attr)]) for i in range(n_tups)] 

def createRelationFile(f_path, f_size, n_attr, block_size, len_attr):
    '''
    Creates a relations csv-tuple file using randmon numbers'
    '''
    # Create 100 really random tuples.
    n_tups = 100
    randTuples = createRandomTuples(n_tups, n_attr, len_attr)

    cur_f_size = 0
    with open(f_path, 'w') as f:
        while cur_f_size < f_size:
            # Sample from the n_tups random tuples created.
            tupleToWrite = '\n'.join(random.sample(randTuples, n_tups))+'\n'
            f.write(tupleToWrite)
            cur_f_size += n_tups*n_attr*len(str(len_attr))
    return

def loadBuffer(f_stream, r_buffer, block_size, tuple_size):
    '''
    Reads a block from the f_stream and appends it to the r_buffer.
    '''
    n_tup = 1
    tup = f_stream.readline()
    if not tup:
        return
    tup = tup.strip()
    while (n_tup*tuple_size) < block_size:
        r_buffer.append(tup)
        tup = f_stream.readline()
        if not tup:
            return
        else:
            tup = tup.strip()
            n_tup+=1
    return

def writeBuffer(f_stream, r_buffer):
    '''
    Flushes out the r_buffer completely into the f_stream.
    '''
    for tup in r_buffer:
        f_stream.write(tup+'\n')
    return

def performSetUnion(out_path, f1_path, f2_path, n_blocks, block_size, ind_type, len_attr, n_attr):
    '''
    Reads contents from relation1 and relation2 into memory blocks and performs union.
    '''
    r1_buffer = []
    r2_buffer = []
    out_buffer = []
    union_hash = []
    f1_over_fl = 0
    f2_over_fl = 0

    # Open file I/O streams
    f1_stream = open(f1_path,'r')
    f2_stream = open(f2_path,'r')
    out_stream = open(out_path,'w')

    # Initialize buffers
    print "\n[INFO] Initializing relation Buffers",
    tuple_size = len_attr*n_attr
    for i in range((n_blocks-1)/2):
        if not f1_over_fl:
            f1_over_fl = loadBuffer(f1_stream, r1_buffer, block_size, tuple_size)
        if not f2_over_fl:
            f2_over_fl = loadBuffer(f2_stream, r2_buffer, block_size, tuple_size)

    n_alloc_block = i+1
    print "...done"
    print "\tRelation1 Buffer : %d records, %d blocks" % (len(r1_buffer),n_alloc_block)
    print "\tRelation2 Buffer : %d records, %d blocks" % (len(r2_buffer),n_alloc_block)

    iter = 0
    while True:
        iter+=1
        if iter%100==33:
            sys.stdout.write('\r')
            sys.stdout.write('Working .')
            sys.stdout.flush()
        elif iter%100==66:
            sys.stdout.write('\r')
            sys.stdout.write('Working ..')
            sys.stdout.flush()
        elif iter%100==0:
            sys.stdout.write('\r')
            sys.stdout.write('Working ...')
            sys.stdout.flush()

        try:
            tup = r1_buffer.pop(0)
            if tup not in union_hash:
                union_hash.append(tup)
                out_buffer.append(tup)
        except:
            f1_over_fl = 1
            
        if len(r1_buffer) < n_alloc_block*(block_size/tuple_size) and not f1_over_fl:
            f1_over_fl = loadBuffer(f1_stream, r1_buffer, block_size, tuple_size)
        if len(out_buffer) >= (block_size/tuple_size):
            print "[INFO] Flushing out_buffer to output to union file"
            writeBuffer(out_stream, out_buffer)

        try:
            tup = r2_buffer.pop(0)
            if tup not in union_hash:
                union_hash.append(tup)
                out_buffer.append(tup)
        except:
            f2_over_fl = 1
            
        if len(r2_buffer) < n_alloc_block*(block_size/tuple_size) and not f2_over_fl:
            f2_over_fl = loadBuffer(f2_stream, r2_buffer, block_size, tuple_size)
        if len(out_buffer) >= (block_size/tuple_size):
            print "[INFO] Flushing out_buffer to output to union file"
            writeBuffer(out_stream, out_buffer)
    
        if f1_over_fl and f2_over_fl:
            print "\n[INFO] Relation tuples read. Flushing remaing out_buffer tuples."
            break

    writeBuffer(out_stream, out_buffer)
    print "[DONE] Union set written successfully"

    f1_stream.close()
    f2_stream.close()
    out_stream.close()
    return

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
    parser.add_argument('-outPath',dest='out_path', type=str)

    args = parser.parse_args()
    return args

def main():
    '''
    Loader function.
    '''
    # Global Variable Definitions
    block_size = 100000 #1e+5
    len_attr = 5

    # Load command line arguments
    args = getArguments()
    out_path = args.out_path
    n_blocks = args.n_blocks
    n_attr = args.n_attr
    ind_type = args.index_type
    
    # Print state
    print "[INFO] Global Variable State :"
    print "\tBlock size  :",block_size
    print "\t#Blocks     :",n_blocks
    print "\tAttr size   :",len_attr
    print "\t#Attributes :",n_attr
    print "\tIndex Type  :",ind_type

    # Create Relation Files
    file1_path = args.file1
    file1_size = args.s_file1
    print "[INFO] Creating random tuples for relation1 at", file1_path,
    createRelationFile(file1_path, file1_size, n_attr, block_size, len_attr)
    print "...done"

    file2_path = args.file2
    file2_size = args.s_file2
    print "[INFO] Creating random tuples for relation2 at", file2_path,
    createRelationFile(file2_path, file2_size, n_attr, block_size, len_attr)
    print "...done"

    performSetUnion(out_path, file1_path, file2_path, n_blocks, block_size, ind_type, len_attr, n_attr)
    print "[EXIT]"
    return

if __name__=='__main__':
    main()
