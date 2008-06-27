from copy import deepcopy

def tic(): 
    import time
    return time.time()

def toc(t=None): 
    import time
    return time.time()-t

def sample():
    r=[
       [1,2,5],
       [2,4],
       [2,3],
       [1,2,4],
       [1,3],
       [2,3],
       [1,3],
       [1,2,3,5],
       [1,2,3]
       ]
    return r

def get_distinct_items(input):
    """
    input: src file, data should be integers
    output: map file key _ value
    returns: d{key:value}
    """
    d=set()
    for line in open(input):
        a=line.split()
        for item in a:
            if item in d:continue
            d.add(item)
    lst=list(d)
    lst2=[int(i) for i in lst]
    lst2.sort()
    return [str(i) for i in lst2]

def space_to_comma(input=None, output=None):
    fout=open(output,'w')
    for line in open(input):
        a=line.split(' ')
        fout.write(','.join(a))
    fout.close()
    
def space_to_sparced_arff(input=None, output=None):
    ditms=get_distinct_items(input)
    
    fout=open(output,'w')
    fout.write('@relation suhel\n')
    fout.write('@attribute 0 {0,1}\n')
    for i in ditms:
        fout.write('@attribute '+i+ ' {0,1}\n')
    fout.write('@data\n')
    
    for line in open(input):
        a=line.split()
        fout.write('{')
        b=[i+' 1' for i in a]
        fout.write(','.join(b)+'}\n')
    fout.close()
    
def arff_to_spaced(infilename=None, outfilename=None ):
    """
    infilename: arff file.
    outfile: output spaced file
    """
    
    fin=open(infilename)
    fout=open(outfilename,'w')
    for line in fin:
        if len(line.strip())==0:continue
        if line.strip()[0] in ['%','@','\n']:continue
        nline=line.replace(",", " ")
        fout.write(nline)
    
    fin.close()
    fout.close()
    fin=None
    fout=None


def spaced_to_arff(infilename=None,outfilename=None,isText=False):
    """
    infilename: spaced file
    outfilename:arff output
    isText: True if attribute is string.
    """
    #d={}
    length=[]
    fin=open(infilename)
    fout=open(outfilename,'w')

    if isText :
        fout.write('@relation r\n')
        fout.write('@attribute 1 string r\n')
        fout.write('@data\n')
        for line in fin:
            ln=line.strip()
            if len(ln)== 0 :continue
            fout.write('"'+ln+'"\n')
        fin.close()
        fout.close()
    else:

        fout_temp=open(outfilename+"_temp",'w')

        #initiate dictionary
        line=fin.next()
        a=line.strip().split(' ')
        n=len(a)
        for i in range(n):
            d[i]=set([a[i]])

        #fill dictionary and write data into temp file            
        for line in fin:
            a=line.strip().split(' ')
            for i in range(n):
                d[i].add(a[i])
            fout_temp.write(','.join(a) +'\n')
        fout_temp.close()

        #add header
        fout.write('@relation r\n')
        for i in range(len(d)):
            d=d[i]
            fout.write('@attribute '+str(i)+' {'+d.pop())
            
            while len(d)>0:
                fout.write(','+d.pop() )
            fout.write('}\n')
        fout.write('@data\n')

        #copy data from temp file to outfilename
        for line in open(outfilename+"_temp"):
            fout.write(line)
        fout_temp.close()
        fout.close()

        #delete temp file
        import os
        os.remove(outfilename+"_temp")

 
def check_duplicates(filename=None):
    """ print the line number----> duplicate item """
    i=0
    for line in open(filename):
        a=line.split(" ")
        i=i+1
        b=set(a[:])
        for item in b:
            if a.count(item) > 1:
                print "line",i,"---->",item
    print "end"


#tests
#check_duplicates("../data/retail.dat")

def subsets(s,r):
    if len(s)== 1:
        pass
    else:
        for item in s:
            subs=s[:]
            subs.remove(item)
            subs.sort()
            if tuple(subs) in r: continue
            r.append(tuple(subs))
            subsets(subs, r)

def subs(lst):
    lst=list(lst)
    r=subset_1(lst)
    lst.sort()
    r.append(tuple(lst))
    return r

def subset_1(lst):
    lst=list(lst)
    r=[]
    subsets(lst, r)
    r.sort()
    return r

def sub_remove_one(lst):
    r=[]
    for item in lst:
        s=lst[:]
        s.remove(item)
        s.sort()
        r.append(tuple(s))
    return r

def get_data(filename="../data/d.txt"):
    """
    return [ [line1] [line2] ... [lineN] ]
    """
    a=[]
    for line in open(filename):
        items=line.split(",")
        a.append(items)
    print "finishd reading file", filename
    return a

def fill_data(data):
    d={}
    for row in data:
        r=subs(row)
        for item in r:
            if item in d:
                d[item]=d[item]+1
            else:
                d[item]=1
    dct={}
    dct[0]=d
    return dct

def fill_k(dct):
    d=dct[0]
    r=dct
    for item in d:
        num=len(item)
        if num in r:
            if item in r[num]:continue
            r[num].append(item)
        else:
            r[num]=[item]
    
    return r

    
def add_item(dct,item):
    """ added to the key dict"""
    if item not in dct[0]:
        print "itemNotFound ",str(item)
        return False

    num=len(item)
    if num in dct:
        if item in dct[num]:
            return False
        else:
            dct[num].append(item)
            return True
    else:
        dct[num]=[item]
        return True

def del_item(dct,item):
    if item not in dct[0]:
        return False
    num = len(item)

    if num in dct:
        if item in dct[num]:
            dct[num].remove(item)
            return True
    else:
        return False
    
def in_item(dct,item):
    num = len(item)
    if num in dct:
        if item in dct[num]:
            return True
    else:
        return False



def support(dct,item):
    num=len(item)
    if item in dct[0]:
        return dct[0][item]
    else:
        raise "itemNotFound",item

def confidence(dct,i1,i2):
    lst=list(i1)
    lst.extend(list(i2))
    #print lst
    lst.sort()
    c1=support(dct, tuple(lst))
    c2=support(dct, i1)
    #print c1,c2
    return float(c1)/float(c2)

def gen_freq(dct,supp):
    import copy
    r=copy.deepcopy(dct)
    #r=dct.copy()
    for k,nv in dct.items():
        for item in nv:
            if support(dct,item)< supp:
                del_item(r,item)
        if len(r[k])==0:
            del r[k]
    return r

def gen_candidate(dct):
    import copy
    r=copy.deepcopy(dct)
    for n in range(2,len(dct)):
        #iterate in row
        for k in dct[n]:
            sub=sub_remove_one(list(k))
            #iterate in subsets
            for item in sub:
                #print item
                if not in_item(dct, item):
                    del_item(r,k)
                    print "dl_item ",str(item)
                    break
        if len(r[n])== 0:
            #delete crules up to len(r)+1
            for j in range(n,len(dct)):
                del r[j]
            break
        
    return r


#confidence(dct, (5,), (1,2,))
def gen_confidence(dct,conf):
    print "generate confideces"
    d=dct[0]
    for i in range(2,len(dct)):
        r=dct[i]
        for j in r:
            print j
            j_left=subset_1(j)
            j_right=[]
            for item in j_left:
                item=list(set(j).difference(set(item)))
                item.sort()
                j_right.append( tuple(item))

            for index in range(len(j_left)):
                c=confidence(dct, j_left[index], j_right[index])
                #print str(j_left[index]),"---->",str(j_right[index])," ,c=", str(c)
                if c>=conf:
                    print str(j_left[index]),"---->",str(j_right[index])," :", support(dct, j_left[index])," ,c=", str(c)
            

                               
    

        

def p(dct,s=None):
    if s != None:
        print "----------------",s,"----------------"
    for k,v in dct.items():
        if k==0:continue
        print k
        print v


def map_data_reverse(mapfile=None, datafile_mapped=None, split_char=" ", datafile=None):
    from time import time
    fmap=open(mapfile)
    fin=open(datafile_mapped)
    fout=open(datafile,'w')
    global d
    #fillin dict with the map
    for line in fmap:
        a=line.split(split_char)
        d[a[1].strip() ]=a[0].strip()
    fmap.close()
    
    for line in fin:
        a=line.split(split_char)
        b=[]
        for item in a:
            b.append(d[item.strip()])
        fout.write( split_char.join(b)+"\n" )
    fin.close()
    fout.close()

def map_data(datafile=None, split_char=" ",datafile_mapped=None,mapfile=None):
    from time import time
    if datafile_mapped == None: datafile_mapped=datafile+"_mapped"
    if mapfile == None: mapfile=datafile+"_map"
    fout=open(datafile_mapped,"w")
    fmap=open(mapfile,"w")
    f=open(datafile)
    d=dict()
    t=tic()
    print "start maping data into integers"
    
    counter=1
    for line in f:
        a=line.split(split_char)
        b=[]
        for item in a:
            if len(item.strip())==0: continue
            if not item in d:
                d[item]=str(counter)
                fmap.write(item.strip()+split_char+str(counter)+"\n")
                counter=counter+1
            b.append(d[item])
        fout.write(split_char.join(b)+"\n")
    f.close()
    fout.close()
    fmap.close()
    
    print "job finished with time elapsed :",toc(t)
    print "you have :",str(counter-1)," distinct items" 
    

if __name__ == '__main__' and False:
          
    from time import *
    #data=sample()
    t1=tic();
    print "1-start maping data to list ",t1
    data=get_data("retail.dat")
    #data=get_data()
    print "1-finish ",toc(t1)
    
    dct=fill_data(data)
    dct=fill_k(dct)
    #p(dct,"dct")
    
    t1=tic()
    print "2-get frequent items"
    #support 0.01
    dct=gen_freq(dct, 16921)
    #dct=gen_freq(dct, 0)    
    print "2-finished at ", toc(t1) 
    #p(dct,"gen freq")
    
    #item=(1,5)
    #del_item(dct,item)
    #p(dct,"del "+str(item))
    
    #dct=gen_freq(dct, 2)
    #p(dct,"gen freq")
    
    t1=tic()
    print "3-get candidate items"
    dct=gen_candidate(dct)
    print "3-finished at ", toc(t1) 
    
    #p(dct,"gen candidate")
    
    t1=tic()
    print "4-generate rules items"
    gen_confidence(dct, 0.70)
    print "4-finished at ", toc(t1)
    
    print "------------------------------------------------------------------"
    t1=tic()
    p(dct,"5- print confidence")
    print "5- finished at ", toc(t1)
