import matplotlib.pyplot as plt
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['axes.linewidth'] = 1.2 #set the value globally
plt.rc('font', family='Helvetica', size=11, weight='roman')
plt.rc('pdf', fonttype=42)

plt.rcParams['axes.linewidth'] = 1.2 #set the value globally
#myfont = FontProperties(fname='SimHei.ttf')
plt.rcParams['font.family']='sans-serif'
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False

dir = '../../Data/Scalability/CluStream/'
fileName = 'Clustream-MicroCluster-KDD98-LatencyPerMc-xClusterSize'
data = pd.read_excel(dir + fileName + '.xlsx')


plt.rcParams['xtick.direction'] = 'in'
plt.rcParams['ytick.direction'] = 'in'
# plt.figure(figsize=(3.8, 2.3))
# plt.subplots_adjust(
#     left=0.11,
#     bottom=0.15,
#     right=0.97,
#     top=0.94,
#     wspace=0.00,
#     hspace=0.00)
fig, ax = plt.subplots(figsize=(3.67, 2.7))

plt.subplots_adjust(
    left=0.14,
    bottom=0.18,
    right=0.97,
    top=0.95,
    wspace=0.00,
    hspace=0.00)



plt.xlabel(U'微簇个数')#, size=8, weight='medium')
#plt.ylabel(U'平均计算时延 ($\mu s$)')#, size=10, weight='medium')
plt.ylabel(U'平均计算时延 (微秒)')#, size=10, weight='medium')
plt.ylim(0, 10)
plt.xlim(15, 160)

marksize = 3
linewidth = 1.2

plt.plot(data[data.columns[0]], data[data.columns[1]], marker='D', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[2]], marker='o', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[3]], marker='s', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[4]], marker='^', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[5]], marker='*', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[6]], marker='p', markersize=marksize, linewidth=linewidth,color='black')

plt.legend(loc=1,  frameon=False, labelspacing=0.2, ncol=3, borderaxespad=0.3, columnspacing=1.2, handletextpad=0.5)
#plt.show()
plt.savefig(dir + fileName + "-Paper1.pdf")
