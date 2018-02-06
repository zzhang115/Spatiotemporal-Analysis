#nam = open('../project/nam/nam_2015.tdv', 'r')
nam = open('../project/beginning-spark/geoheight.txt', 'r')
geoheightkml = open('./geoheightkml.txt', 'w')
for line in nam:
    geoheightkml.write(line.split('\t')[0] + '\t' + str(int(float(line.split('\t')[1]))) + '\n')
