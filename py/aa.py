#coding:utf-8

from matplotlib import style
import matplotlib.pyplot as plt
import codecs

style.use("ggplot")


category_number = {}
with open("bb.txt" , "rt",encoding='UTF-8') as fp:
    for line in fp.readlines():
        # print(line.strip().split(",")[3])
        # print(int(line.strip().split(",")[1]))
        category_number[line.strip().split(",")[3]] = int(line.strip().split(",")[1])

# print(category_number)
# print(type(category_number))

category = []
number = []
for k in category_number:
    # print(k)
    # print(category_number[k])
    category.append(k)
    number.append(category_number[k])

fig, ax = plt.subplots()

rects1 = plt.bar(range(1,len(category)+1), number, width=0.4, alpha=0.2, color='g',align="center") 

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2.0, 1.05*height,'%d'%int(height), ha='center', va='bottom')

autolabel(rects1)

plt.xlabel(u"dayId", color='r')  # 横坐标
plt.ylabel(u"pv-eu", color='r')  # 纵坐标
plt.title(u"Recent 7-day trend chart")  # 图片名字
plt.xlabel(u"dayId",color='r') 
plt.ylabel(u"eu",color='r')     
plt.xticks(range(1,len(category)+1),category,fontsize=6)      
plt.yticks(fontsize=10)        
plt.savefig(u'top10category.png')
plt.show()    
