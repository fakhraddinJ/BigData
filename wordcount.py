import time, re
from pyspark import SparkContext, SparkConf

def linesToWordsFunc(line):
    wordsList = line.split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
    filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    return filtered

def wordsToPairsFunc(word):
    return (word, 1)

def reduceToCount(a, b):
    return (a + b)

def main():
    conf = SparkConf().setAppName("Wordscount").setMaster("local")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("hdfs:///user/hdfs/*.txt")

    words = rdd.flatMap(linesToWordsFunc)
    pairs = words.map(wordsToPairsFunc)
    counts = pairs.reduceByKey(reduceToCount)

    # Listar solo las 100 primeras
    #output = counts.takeOrdered(100, lambda (k, v): -v)

    for(word, count) in counts:
        print word + ': ' + str(count)
    
    counts.coalesce(1).saveAsTextFile("output.txt")
    sc.stop()

if __name__ == "__main__":
    main()