from pyspark.sql import SparkSession
from readdatautility  import ReadDataUtil
from pyspark.sql.types import *


if __name__=='__main__':
    spark = SparkSession.builder\
           .master("local[*]")\
            .appName("Airline Data Mgmt")\
            .getOrCreate()
    rdu=ReadDataUtil()
    airlineschema = StructType([StructField("airline_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("alias", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("callsign", StringType()),
                                StructField("country", StringType()),
                                StructField("active", StringType())])
    df = rdu.readCsv(spark=spark,path=r"C:\airline data\airline.csv",
                    schema=airlineschema)
    df.show()
    df.printSchema()


