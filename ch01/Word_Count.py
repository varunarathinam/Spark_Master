import sys
from pyspark import SparkContext

if __name__ == "__main__":
  sc = SparkContext("local[*]", "wordcount")
  sc.setLogLevel("ERROR")
  lines = sc.textFile("D://Spark_Programming//ch01//input_data.txt")
  words = lines.flatMap(lambda line: line.split(" "))
  
  wordCounts = words.countByValue()
  for word, count in wordCounts.items():
    print(word, count)
