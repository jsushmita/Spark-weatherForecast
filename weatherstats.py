#!/usr/bin/env python
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

# create Spark context with Spark configuration
context = SparkContext.getOrCreate()
sqlContext = SQLContext(context)

def makeDf(filename):
    # read text file and convert eaach line to row
    lines = context.textFile(filename)
    parts = lines.map(lambda x: x.split(','))
    weathertable = parts.map(lambda r: Row(STATION=r[0], DATE=r[1], MEASUREMENTS=r[2],
                                   DEGC=int(r[3]), MFLAG=r[4], QFLAG=r[5], SFLAG=r[6],
                                   TIME=r[7]))
    # Infer the schema, and register the DataFrame as a table.
    df = sqlContext.createDataFrame(weathertable)
    # remove abnormalities and missing data
    return df.filter(df.QFLAG=='') 


def makeStationsDf(filename):
    lines = context.textFile(filename)
    parts = lines.map(lambda x: x.split(','))
    table = parts.map(lambda r: Row(STATEID=r[0], LAT=r[1], LON=r[2]))
    return sqlContext.createDataFrame(table)


def run():
    import pyspark.sql.functions as sqlf

    stations = makeStationsDf('hdfs:/user/jadhavsi/ghcnd-stations.csv') 
    
    for year in range(2000,2020):
        dataframe = makeDf("hdfs:/user/tatavag/weather/"+str(year)+".csv")

        print("\n%s\n~~~~\n" % year)

        # Average TMIN
        r = dataframe.filter(dataframe.MEASUREMENTS=='TMIN').groupBy().avg('DEGC').first()
        print('Average TMIN = %0.1f degrees C' % (r['avg(DEGC)'] / 10.0))

        # Average TMAX
        r = dataframe.filter(dataframe.MEASUREMENTS=='TMAX').groupBy().avg('DEGC').first()
        print('Average TMAX = %0.1f degrees C' % (r['avg(DEGC)'] / 10.0))

        # MAX TMAX
        r = dataframe.filter(dataframe.MEASUREMENTS=='TMAX').groupBy().max('DEGC').first()
        max_tmax = float(r['max(DEGC)']) / 10.0
        print('Maximum TMAX : %0.1f degrees C' % (max_tmax))

        # MIN TMIN
        r = dataframe.filter(dataframe.MEASUREMENTS=='TMIN').groupBy().min('DEGC').first()
        min_tmax = float(r['min(DEGC)']) / 10.0
        print('Minimum TMIN : %0.1f degrees C' % (min_tmax))

        # Five hottest stations (on average)
        hottestFive = dataframe.filter(dataframe.MEASUREMENTS=='TMAX') \
                    .groupBy(dataframe.STATION) \
                    .agg(sqlf.avg('DEGC')) \
                    .sort(sqlf.desc('avg(DEGC)')) \
                    .limit(5).collect()
        print()
        
        i = 1
        for s in hottestFive:
            t = float(s['avg(DEGC)']) / 10.0
            print('Hottest station #%s: %s - %0.1f degrees C'
                  % (i, s.STATION, t))
            i = i + 1

        # Five coldest stations (on average)
        coldestFive = dataframe.filter(dataframe.MEASUREMENTS=='TMIN') \
                     .groupBy(dataframe.STATION) \
                     .agg(sqlf.avg('DEGC')) \
                     .sort(sqlf.asc('avg(DEGC)')) \
                     .limit(5).collect()
        print()
        i = 1
        for s in coldestFive:
            t = float(s['avg(DEGC)']) / 10.0
            print('Coldest station #%s: %s - %0.1f degrees C'
                  % (i, s.STATION, t))
            i = i + 1

        print()
        medianTX = dataframe.filter(dataframe.MEASUREMENTS=='TMAX').approxQuantile('DEGC', [0.5], 0.25)
        print('Median TMAX : %0.1f degrees C' % (medianTX[0] / 10.0))

        print()
        medianTM = dataframe.filter(dataframe.MEASUREMENTS=='TMIN').approxQuantile('DEGC', [0.5], 0.25)
        print('Median TMIN : %0.1f degrees C' % (medianTM[0] / 10.0))




def runFullDataset():
    import pyspark.sql.functions as sqlf
    from datetime import datetime as dt

    stations = makeStationsDf('hdfs:/user/jadhavsi/ghcnd-stations.csv')

    # Hottest and coldest day in the entire dataset
    print("\nFor the entire dataset (2000-2019)\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    df = makeDf("hdfs:/user/tatavag/weather/20??.csv")

    #COLDEST DAY AND STATION
    coldest = df.filter(df.MEASUREMENTS=='TMIN').groupBy('STATION', 'DATE').min('DEGC') \
                .sort(sqlf.asc('min(DEGC)')).first()

    date = dt.strptime(coldest.DATE, '%Y%m%d').strftime('%d %b %Y')
    city = getcity(stations, coldest.STATION)

    print('Coldest station was %s on %s: %0.1f deg C'
          % (coldest.STATION, date, float(coldest['min(DEGC)']) / 10.0))

    # HOTTEST DAY AND STATION
    hottest = df.filter(df.MEASUREMENTS=='TMAX').groupBy('STATION', 'DATE').max('DEGC') \
                .sort(sqlf.desc('max(DEGC)')).first()

    date = dt.strptime(hottest.DATE, '%Y%m%d').strftime('%d %b %Y')
    city = getcity(stations, hottest.STATION)

    print('Hottest station was %s on %s: %0.1f deg C'
          % (hottest.STATION, date, float(hottest['max(DEGC)']) / 10.0))
    print()

    # Median Tmax of entire Dataset
    medianTX = df.filter(df.MEASUREMENTS=='TMAX').approxQuantile('DEGC', [0.5], 0.25)
    print('Median TMAX of entire dataset in degrees C')
    print(medianTX[0]/10.0)

    print()

    # Median Tmin of entire Dataset
    medianTM = df.filter(df.MEASUREMENTS=='TMIN').approxQuantile('DEGC', [0.5], 0.25)
    print('Median TMIN of entire dataset in degrees C')
    print(medianTM[0]/10.0)

if __name__ == '__main__':
    run()
