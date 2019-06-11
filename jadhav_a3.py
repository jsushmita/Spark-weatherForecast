import sys
from random import random
from operator import add
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import DataFrameWriter

if __name__ == "__main__":
	# create Spark context with Spark configuration
	context = SparkContext(appName="WeatherAnalysis")
	sqlContext = SQLContext(context)
	for year in range(2000,2020):

		# provide path to input file
		input = "hdfs:/user/tatavag/weather/"+str(year)+".csv"

		# read text file and convert eaach line to row
		lines = context.textFile(input)
		
		#  collect RDD to a list
		parts = lines.map(lambda l: l.split(","))

		# create list of row data
		val = parts.map(lambda p: Row(ID=p[0], DATE=p[1], ELEMENT=p[2], VALUE=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))

		# create Spark Dataframe
		weatherdata = sqlContext.createDataFrame(val)

		# create in-memory table in columnar format
		weatherdata.registerTempTable("weather_table")

		# SQL can be run over DataFrames that have been registered as a table.
		result_1 = sqlContext.sql("SELECT ELEMENT, avg(VALUE) VALUE from weather_table where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ELEMENT")
		result_1.show()
		analysistwo = sqlContext.sql("SELECT ELEMENT, IF(ELEMENT='TMAX',MAX(VALUE), MIN(VALUE)) VALUE from weather_table where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ELEMENT")
		analysistwo.show()
		analysisthreeA = sqlContext.sql("SELECT ID, MIN(VALUE) as Val from weather_table where ELEMENT='TMIN' AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' GROUP BY ID ORDER BY Val LIMIT 5")
		analysisthreeA.show()
		analysisthreeB = sqlContext.sql("SELECT ID, MAX(VALUE) as Val from weather_table where ELEMENT='TMAX' AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' GROUP BY ID ORDER BY Val DESC LIMIT 5")
		analysisthreeB.show()
		analysisfourA = sqlContext.sql("SELECT ID, DATE, VALUE from weather_table where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ID, DATE, VALUE ORDER BY VALUE LIMIT 1")
		analysisfourA.show()
		analysisfourB = sqlContext.sql("SELECT ID, DATE, VALUE from weather_table where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ID, DATE, VALUE ORDER BY VALUE DESC LIMIT 1")
		analysisfourB.show()
		result_1.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res1/"+str(year)+"result.csv")
		analysistwo.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res2/"+str(year)+"result.csv")
		analysisthreeA.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res31/"+str(year)+"result.csv")
		analysisthreeB.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res32/"+str(year)+"result.csv")
		analysisfourA.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res41/"+str(year)+"result.csv")
		analysisfourB.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/jadhavsi/res42/"+str(year)+"result.csv")
	context.stop()

	with open("hdfs:/user/jadhavsi/output/AvgTminTmax.csv", 'w') as outfile:
		for i in range(0,20):
			fname = "hdfs:/user/jadhavsi/res1/"+str(2000+i)+"result.csv"
			with open(fname) as infile:
				outfile.write(infile.read())
	

