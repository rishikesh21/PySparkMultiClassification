from pyspark.sql import SQLContext
from pyspark import SparkContext


global sqlContext
def InitializeSparkContext(self):
    sc = SparkContext()
    self.sqlContext = SQLContext(sc)

def LoadFile():
        data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('D:\\Log.csv')
        data.show(10)

def _init_(self):
    self

if __name__ == '__main__':
    _init_()
    InitializeSparkContext()
    LoadFile()
