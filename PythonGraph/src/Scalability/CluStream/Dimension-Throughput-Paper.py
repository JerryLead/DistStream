import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['axes.linewidth'] = 1.2 #set the value globally

plt.rcParams['axes.linewidth'] = 1.2 #set the value globally
#myfont = FontProperties(fname='SimHei.ttf')
plt.rcParams['font.family']='sans-serif'
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus']=False

#plt.rc('font', family='Helvetica', size=11, weight='roman')
plt.rc('pdf', fonttype=42)
dir = '../../Data/Scalability/CluStream/'
fileName = 'Clustream-Dimension-KDD98-Throughput-xParallelism-Paper'
data = pd.read_excel(dir + fileName + '.xlsx')

plt.rcParams['xtick.direction'] = 'in'
plt.rcParams['ytick.direction'] = 'in'
plt.figure(figsize=(3.6, 2.7))
plt.subplots_adjust(
    left=0.17,
    bottom=0.15,
    right=0.97,
    top=0.94,
    wspace=0.00,
    hspace=0.00)

# plt.xticks(fontsize=8, weight='medium')
# plt.yticks(fontsize=8, weight='medium')
plt.xlabel(U'并行度')#, size=8, weight='medium')
plt.ylabel(U'吞吐率 (x' +'${10^3}$' + '数据/秒)')

marksize = 3
linewidth = 1.2
plt.ylim(0, 150)

plt.plot(data[data.columns[0]], data[data.columns[1]], marker='D', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[2]], marker='o', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[3]], marker='s', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[4]], marker='^', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[5]], marker='*', markersize=marksize, linewidth=linewidth,color='black')
plt.plot(data[data.columns[0]], data[data.columns[6]], marker='p', markersize=marksize, linewidth=linewidth,color='black')

plt.legend(loc='best', frameon=False, labelspacing=0.3, borderaxespad=0.2, columnspacing=2.2, handletextpad=0.5)
#plt.show()
plt.savefig(dir + fileName + "+Paper.pdf")

