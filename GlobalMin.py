from pyspark import SparkConf, SparkContext
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

conf = SparkConf().setMaster("local").setAppName("GlobalMax")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    country = fields[3]
    avgtemperature = fields[1]
    return (country, avgtemperature)

lines = sc.textFile("file:///SparkCourse/GlobalLandTemperaturesByCountry.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

for result in results:
    print result[0] + " " + result[1]
