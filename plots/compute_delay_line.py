import math
from scipy import linalg
import numpy as np
#import matplotlib.pyplot as plt
import sys, os, random

x_data = []
y_data = []
xLable = ""
yLable = ""


testX = [1,2,3,4,5,6]
testY = [1,2,3,4,5,6]
def readFile(f):
    for line in f.readlines():
        xi = float(line.split()[7].split(":")[1])
        yi = float(line.split()[9])
        ###there are some fix for data
        #if yi == 0 or yi > 61*24 :
        #if yi == 0 or xi == 0 :
        #    continue
        #if xi == 0 :
        #    continue
        ####
        x_data.append(xi)
        y_data.append(yi)

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()


def plot(dataname, savename):
    #fig = plt.figure(figsize=(7,4))
    #ax = fig.add_subplot(111)
    #fig.tight_layout(pad=3)
    #
    #ax.plot(x_data, y_data, 'ro')

    a = np.mat([x_data,[1]*len(x_data)]).T
    b = np.mat(y_data).T
    (t,res,rank,s) = linalg.lstsq(a,b)
    print t

    r = t[0][0]
    c = t[1][0]
    tmp_str = "the formula is: Y = %.2f*X + %.2f, from data: %s, plot in: %s.\n"\
              % (r, c, dataname, savename)
    log_write(tmp_str)
    #x_ = x_data
    #y_ = [r*a+c for a in x_data]
    #ax.plot(x_,y_,"b-")



    #plt.savefig(savename)
    #plt.show()
    #


def main():
    if len(sys.argv) != 3 :
        print "Error\nUsage:\n plots.py [filename] [savename]"
        return
    f = open(sys.argv[1])
    savename = sys.argv[2]
    readFile(f)


    plot(sys.argv[1], savename)
    f.close()

if __name__ == "__main__":
    main()

