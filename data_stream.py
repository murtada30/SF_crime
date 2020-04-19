import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id",StringType(),True),
                     StructField("original_crime_type_name",StringType(),True),
                     StructField("report_date",StringType(),True),
                     StructField("call_date",StringType(),True),
                     StructField("offense_date",StringType(),True),
                     StructField("call_time",StringType(),True),
                     StructField("call_date_time",TimestampType(),True),
                     StructField("disposition",StringType(),True),
                     StructField("address",StringType(),True),
                     StructField("city",StringType(),True),
                     StructField("state",StringType(),True),
                     StructField("agency_id",StringType(),True),
                     StructField("address_type",StringType(),True),
                     StructField("common_location",StringType(),True)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    #spark.sparkContext.setLogLevel("WARN");
    
    
    df= spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.data.streaming.murtada.sfc.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetPerTrigger", 100) \
        .option("stopGracefullyOnShutdown", "true")\
        .load()

    # Show schema for the incoming resources for checks
    #logger.warning("****************************")
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition","call_date_time") \
                                  .distinct()

    
    # count the number of original crime type
    agg_df = distinct_table \
        .select("original_crime_type_name","call_date_time")\
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(psf.window("call_date_time", "10 minutes", "5 minutes"),"original_crime_type_name")\
        .count()
        
    
    #example from https://hackingandslacking.com/pyspark-macro-dataframe-methods-join-and-groupby-477a57836ff
    #https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
            .writeStream \
            .outputMode("complete")\
            .format("console")\
            .start();


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition").collect();

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df,agg_df.disposition == radio_code_df.disposition, "inner" )

    #joinedDF = customersDF.join(ordersDF, customersDF.name == ordersDF.customer)
    #example from https://hackingandslacking.com/pyspark-macro-dataframe-methods-join-and-groupby-477a57836ff


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()
    
    ##spark.conf.set('spark.executor.memory', '3g')
    ##spark.conf.set('spark.executor.cores', '3g')

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
