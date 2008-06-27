def addlines(src=None,dis=None):
    """ add line numbers as follows
        #line: .....line data.....
    """
    print 'addlines('+src+', '+dis+' )'
    from time import time
    tic=time()
    print "start time:",tic
    fout=open(dis,"w")
    fin=open(src)
    counter=1
    for line in fin:
        line=str(counter)+":"+line
        fout.write(line)
        counter=counter+1
    fin.close()
    fout.close()
    toc=time()
    print "End time:",toc
    print "Time elapsed: ", (tic-toc)
    print "number of rows: ",counter-1

    return counter-1

if __name__ == '__main__':
    import sys
    a=sys.argv
    addlines(a[0], a[1])