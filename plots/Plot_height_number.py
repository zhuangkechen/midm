#!/usr/bin/env python
"""
This will be show soon.
plot the weibo friend relationship 
the indegree with the number (PDF)

"""
__author__ = """Zhuang Kechen (zhuangkechen@gmail.com)"""
__date__ = "$Date: 2012-10-30 $"
__credits__ = """"""
__revision__ = "$Revision: 0.1$"
#    Copyright (C) 2012 by
#    Zhuang Kechen <zhuangkechen@gmail.com>
#    All rights reserved.
#    BSD license.


import os,sys
import matplotlib.pyplot as plt 
import numpy as np
from scipy import linalg
import math



#some value
count_all = 0
indegree = []
in_number = []
pdf = []
sort_dict = dict()
all_number=0

step = 1
rank = 0
percent = 0

def do_something(file_object):
    count_all = 0
    for line in file_object:
        if line.split()[0] in ['#']:
            print "a comment...\n"
        else:
            a = int(line.split()[0])
            b = int(line.split()[1])
            if a > 0 and a<100000 and b>0:
	            temp = {a: b}
	            sort_dict.update(temp)
	            count_all = count_all + b
	            #indegree.append(a)
	            #in_number.append(b)
    print('all user number: %d' % (count_all))
    degree_keys = sort_dict.keys()
    degree_keys.sort()
    temp = 0
    all_number = 0
    for i in degree_keys:
		some_temp = 1-float(temp)/float(count_all)
		if some_temp > 0:
		    indegree.append(i)
		    in_number.append(sort_dict[i])
		    temp = temp + sort_dict[i]
		    pdf.append(some_temp)
		    all_number = all_number+1
    print(pdf)
    print(indegree)
    for v in pdf:
        if v <=0:
			print("WHATTTTTTTT")
			print(v)
    #print(indegree)
    #print(pdf)
		
            
def show_all():
    #calculate_density()
    #Linear regressison -polyfit - polyfit can be used other orders polys
    
    #logx = [np.log(f) for f in indegree]
    #logy = [np.log(f) for f in pdf]
    #a = np.mat([logx,[1]*len(indegree)]).T
    #b = np.mat(logy).T
    #(t,res,rank,s) = linalg.lstsq(a,b)
    #r = t[0][0]
    #c = t[1][0]
    #x_ = indegree
    #y_ = [10**(r*a+c) for a in logx]
    
    logx = [math.log(f,math.e) for f in indegree]
    logy = [math.log(f,math.e) for f in in_number]
    a = np.mat([logx,[1]*len(indegree)]).T
    b = np.mat(logy).T
    (t,res,rank,s) = linalg.lstsq(a,b)
    r = t[0][0]
    c = t[1][0]
    x_ = indegree
    y_ = [math.e**(r*a+c) for a in logx]
    
	
    #print('all_number = %d ' % (all_number))
    fig = plt.figure(figsize=(7,4))
    ax = fig.add_subplot(111)
    fig.tight_layout(pad=3)
    #plt.xscale('log')
    plt.yscale('log')
    #ax.loglog(indegree,in_number,"o",c='w')
    #plt.title('Retweets Cascades Tree Height with the Cascades Count')
    plt.xticks(fontsize=14) 
    plt.yticks(fontsize=14)
    #plt.axis([1, 10**5, 0.5, 10**8])
    #plt.scatter(indegree, in_number, facecolors='none', edgecolors='b')
    plt.plot(indegree, in_number,"o-", c="b")
    string_tmp = 'y=%ex^%.2f' %(c,r)
    #plt.plot(x_,y_,"r-",label=string_tmp)
    #plt.legend(loc='upper right')
    plt.grid(True) 
    plt.xlabel('Retweets Cascades Height',fontsize=14)
    plt.ylabel('Count',fontsize=14)
    plt.savefig('height-number-count.png')
    plt.show()
    

if __name__ == '__main__':
    nargs = len(sys.argv)
    if not nargs==2:
        print "Input Error\n"
    else:
        infile = open(sys.argv[1])
        #outfile = open(sys.argv[2], 'w')
        do_something(infile)
        show_all()
        #outfile.close()
        infile.close()
