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
        xi = line.split()[0]
        yi = float(line.split()[1])*(10**4)
        x_data.append(xi)
        y_data.append(yi)

def x_data_change():
    xmin = min(x_data)
    for i in range(0, len(x_data)):
        x_data[i] = x_data[i]-xmin

def x_data_to_day():
    for i in range(0, len(x_data)):
        x_data[i] = int(x_data[i]/24)

def plot(savename, keyword, xLable, yLable):
    fig = plt.figure(figsize=(7,4))
    ax = fig.add_subplot(111)
    fig.tight_layout(pad=3)
    #
    x_pos = range(len(x_data))
    #plt.barh(y_data, x_pos,  align='center', alpha=0.4)
    plt.bar(x_pos, y_data, align='center', width=0.4)
    plt.xticks(x_pos, x_data)
    plt.ylabel(yLable)
    #plt.title(keyword)

    plt.savefig(savename)
    plt.show()
    #


def main():
    if len(sys.argv) != 6 :
        print "Error\nUsage:\n plots.py [filename] [savename] [title] [xLable] [yLable]"
        return
    f = open(sys.argv[1])
    savename = sys.argv[2]
    keyword = sys.argv[3]
    xLable = sys.argv[4]
    yLable = sys.argv[5]
    readFile(f)
    #x_data_to_day() #change the data from hourtime to daytime form
    #x_data_change()
    plot(savename, keyword, xLable, yLable)
    f.close()

if __name__ == "__main__":
    main()

