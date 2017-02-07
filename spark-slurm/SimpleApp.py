from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: SimpleApp.py <input_file> <output_file>", file=sys.stderr)
    exit(-1)

  textFileName = sys.argv[1] 
  outputDir = sys.argv[2]

  sc = SparkContext()
  textData = sc.textFile(textFileName)

  wordFrequencies = textData.flatMap(lambda x: x.split(" ")) \
      .map (lambda word: (word, 1)) \
      .reduceByKey(add)

  summary = wordFrequencies.takeOrdered(100, key = lambda x: -x[1])

  summary.foreach(print)

  # Writes results to file
  summaryRDD = sc.makeRDD(summary, 1)
  summaryRDD.saveAsTextFile(outputDir)
