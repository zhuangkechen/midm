import math
from scipy import linalg
import numpy as np
import matplotlib.pyplot as plt
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
        if yi == 0 or yi > 61*24 :
            continue
        #if xi == 0 :
        #    continue
        ####
        x_data.append(xi)
        y_data.append(yi)

def x_data_change():
    xmin = min(x_data)
    for i in range(0, len(x_data)):
        x_data[i] = x_data[i]-xmin

def plot(savename):
    fig = plt.figure(figsize=(7,4))
    ax = fig.add_subplot(111)
    fig.tight_layout(pad=3)
    #
    ax.plot(x_data, y_data, 'ro')

    a = np.mat([x_data,[1]*len(x_data)]).T
    b = np.mat(y_data).T
    (t,res,rank,s) = linalg.lstsq(a,b)
    print t

    r = t[0][0]
    c = t[1][0]
    x_ = x_data
    y_ = [r*a+c for a in x_data]
    ax.plot(x_,y_,"b-")



    plt.savefig(savename)
    plt.show()
    #


def main():
    if len(sys.argv) != 3 :
        print "Error\nUsage:\n plots.py [filename] [savename]"
        return
    f = open(sys.argv[1])
    savename = sys.argv[2]
    readFile(f)


    plot(savename)
    f.close()

if __name__ == "__main__":
    main()

