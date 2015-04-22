import math
import numpy as np
import matplotlib.pyplot as plt
import sys, os, random

dict1 = {}
dict2 = {}

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
        if xi <= 32 :
            continue
        #if xi == 0 :
        #    continue
        ####
        if label == 1 :
            dict1[xi] = yi
        else :
            dict2[xi] = yi

def sortData():
    d1 = sorted(dict1.iteritems())
    d2 = sorted(dict2.iteritems())
    count = 0
    for i in d1 :
        count = count + 1
        x1_data.append(count)
        y1_data.append(i[1])
    count = 0
    for i in d2 :
        count = count + 1
        x2_data.append(count)
        y2_data.append(i[1])

def log_write(counts):
    f = open("log.txt", 'a')
    f.write(counts)
    f.close()


def plot(savename):
    fig = plt.figure(figsize=(7,4))
    ax = fig.add_subplot(111)
    fig.tight_layout(pad=3)
    #
    plt.plot(x1_data, y1_data, color='blue', linestyle='-', linewidth=2, \
             label="real")#, marker='o')
    plt.plot(x2_data, y2_data, color='red', linestyle='--', linewidth=2, \
             label="predicted")#, marker='^')
    plt.legend(loc='upper right')
    plt.grid(True)
    plt.xlabel('Time(days)',fontsize=14)
    plt.ylabel('Tweets Population',fontsize=14)
    #plt.plot(x1_data, y1_data, 'b-')
    #plt.plot(x2_data, y2_data, 'r-')
    plt.savefig(savename)
    plt.show()
    #


def main():
    if len(sys.argv) != 4 :
        print "Error\nUsage:\n plots.py [realfilename] [predictfilename] [savename]"
        return
    f1 = open(sys.argv[1])
    f2 = open(sys.argv[2])
    savename = sys.argv[3]
    readFile(1, f1)
    readFile(2, f2)
    sortData()

    plot(savename)
    f1.close()
    f2.close()

if __name__ == "__main__":
    main()

