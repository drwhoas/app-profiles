#Code adapted from 'Hadoop with Python by Zach Radtka, Donald Miner' by O'Reilly

from pyspark import SparkContext
def main():

   sc = SparkContext(appName='SparkWordCount')
   input_file = sc.textFile('/user/cloudera/input/input.txt') 
   group=input_file.flatMap(lambda line: line.split())
   length = len(group)
   word_list=[]
   for count1 in range(length-4):
        count2=count1+5
        word_list.append(''.join(group[count1:count2])

   sequence=word_list.map(lambda word: (word, 1)) \
               .reduceByKey(lambda a,b: a + b)
   
   sequence.saveAsTextFile('/user/cloudera/output/Test')

   sc.stop()

if __name__ == '__main__':
   main()




