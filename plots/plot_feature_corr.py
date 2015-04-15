from bokeh.plotting import *
import sys, os, random

colors = "0123456789ABCDEF"
x_data = []
y_data = []
xLable = ""
yLable = ""


testX = [1,2,3,4,5,6]
testY = [1,2,3,4,5,6]
def readFile(f):
    for line in f.readlines():
        xi = int(line.split()[0])
        yi = int(line.split()[1])
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
    htmlname = "%s.html" % (savename)
    lengend_name = keyword.replace("_", " ")
    output_file(htmlname, title=lengend_name)


    tmp_color = "#"
    for i in range(0, 6):
        j = random.randint(0, len(colors)-1)
        tmp_color = tmp_color+colors[j]
    p = figure(plot_width=1000, plot_height=500, title="Topics temporal dynamics")
    p.xaxis.axis_label = xLable
    p.yaxis.axis_label = yLable
    p.line(x_data, y_data, color=tmp_color, line_width=2, legend=lengend_name)
    #save(p)
    show(p)
    #save(p)

def main():
    if len(sys.argv) != 6 :
        print "Error\nUsage:\n plots.py [filename] [savename] [lengendname] [xLable] [yLable]"
        return
    f = open(sys.argv[1])
    savename = sys.argv[2]
    keyword = sys.argv[3]
    xLable = sys.argv[4]
    yLable = sys.argv[5]
    readFile(f)
    #x_data_to_day() #change the data from hourtime to daytime form
    x_data_change()
    plot(savename, keyword, xLable, yLable)
    f.close()

if __name__ == "__main__":
    main()

