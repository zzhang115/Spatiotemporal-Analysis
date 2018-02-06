import math
from collections import OrderedDict

matrix = open('./matrix', 'r')
feature = open('./features.txt', 'r')

features = []
for line in feature:
    line = line.replace('\n', '')
    features.append(line)

matrixs = []
for line in matrix:
    line = line.replace('\n', '')
    cors = line.split()
    for cor in cors:
        matrixs.append(float(cor))

# print(matrixs)
dict = {}
count = 0
for i in range(len(features)):
    for j in range(len(features)):
        if i > 1 and j > 1:
            dict[features[i] + ' - ' + str(features[j])] = matrixs[count]
            count += 1

sorted_dict= OrderedDict(sorted(dict.items(), key=lambda t: -abs(t[1])))

for item in sorted_dict:
    print(item, '\t', sorted_dict[item])
