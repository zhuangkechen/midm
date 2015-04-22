import math
from scipy import linalg
import numpy as np
import matplotlib.pyplot as plt
import sys, os, random

x1_data = []
y1_data = []
x2_data = []
y2_data = []
xLable = ""
yLable = ""


testX = [1,2,3,4,5,6]
testY = [1,2,3,4,5,6]
def readFile(label, f):
    for line in f.readlines():
        xi = int(line.split(",")[0].split("(")[1])
        yi = int(line.split()[1].split(")")[0])
        ###there are some fix for data
        if xi >= 25 :
            continue
        #if xi == 0 :
        #    continue
        ####
        if label == 1 :
            x1_data.append(xi)
            y1_data.append(yi)
        else :
            x2_data.append(xi)
            y2_data.append(yi)

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()


def plot(dataname, savename):
    fig = plt.figure(figsize=(7,4))
    ax = fig.add_subplot(111)
    fig.tight_layout(pad=3)
    #
    ax.plot(x1_data, y1_data, 'b-')
    ax.plot(x2_data, y2_data, 'r-')
    plt.savefig(savename)
    #plt.show()
    #


def main():
    if len(sys.argv) != 4 :
        print "Error\nUsage:\n plots.py [realfilename] [predictfilename] [savename]"
        return
    f1 = open(sys.argv[1])
    f2 = open(sys.argv[1])
    savename = sys.argv[2]
    readFile(1, f1)
    readFile(2, f2)


    plot(sys.argv[1], savename)
    f.close()

if __name__ == "__main__":
    main()

