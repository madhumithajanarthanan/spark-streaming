
# Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 kafka_consumer_iot_agg_to_console.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SQLContext
from pyspark import SparkContext
import pandas as pd
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
#from pyspark.ml.feature import OneHotEncoderEstimator


# Set Kafka config
kafka_broker_hostname='localhost'
kafka_consumer_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_consumer_portno
kafka_topic_input='air_quality'


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("Air quality").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Pull data from Kafka topic
    consumer = KafkaConsumer(kafka_topic_input)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_input) \
        .load()

    # Convert data from Kafka broker into String type
    df_kafka_string = df_kafka.selectExpr("CAST(value AS STRING) as value")

    # Define schema to read JSON format data
    air_schema = StructType()\
        .add("City", StringType()) \
        .add("Datetime", StringType()) \
        .add("PM2_5", DoubleType()) \
        .add("PM10", DoubleType()) \
        .add("NO", DoubleType()) \
        .add("NO2", DoubleType()) \
        .add("NOx", DoubleType()) \
        .add("NH3", DoubleType()) \
        .add("CO", DoubleType()) \
        .add("SO2", DoubleType()) \
        .add("O3", DoubleType()) \
        .add("Benzene", DoubleType()) \
        .add("Toluene", DoubleType()) \
        .add("Xylene", DoubleType()) \
        .add("AQI", DoubleType()) \
        .add("AQI_Bucket", StringType()) 

    # Parse JSON data
    df_kafka_string_parsed = df_kafka_string.select(from_json(df_kafka_string.value, air_schema).alias("air_data"))

    df_kafka_string_parsed_formatted = df_kafka_string_parsed.select(
        col("air_data.City").alias("City"),
        col("air_data.Datetime").alias("Date"),
        col("air_data.PM2_5").alias("PM2_5"),
        col("air_data.PM10").alias("PM10"),
        col("air_data.NO").alias("NO"),
        col("air_data.NO2").alias("NO2"),
        col("air_data.NOx").alias("NOx"),
        col("air_data.NH3").alias("NH3"),
        col("air_data.CO").alias("CO"),
        col("air_data.SO2").alias("SO2"),
        col("air_data.O3").alias("O3"),
        col("air_data.Benzene").alias("Benzene"),
        col("air_data.Toluene").alias("Toluene"),
        col("air_data.Xylene").alias("Xylene"),
        col("air_data.AQI").alias("AQI"),
        col("air_data.AQI_Bucket").alias("AQI_Bucket"))
    #sDF = sql.createDataFrame(df_kafka_string_parsed_formatted)
    #lr= LogisticRegression(maxIter=10,regParam=0.01)
    def get_prediction(df, batchId):
        if batchId==0:
            return    
        print("Hello ! This is Batch: ",batchId)
        #df= df.na.replace(["poor","good"], ["0","1"], "AQI_Bucket").show()
        #df.show()
        #df.na.fill(value=0.0)
        #print(type(df))
        df = df.withColumn("label", when(df.AQI_Bucket == "Poor","0").when(df.AQI_Bucket =="Good","1").otherwise(df.AQI_Bucket))
        #df1= StringIndexer().setInputCol("AQI_Bucket").setOutputCol("label")
        #df1_model= df1.fit(df)
        #df= df1_model.transform(df)
        df.show()
        assembler = VectorAssembler(inputCols=['PM2_5','PM10','NO','NO2','NOx','NH3','CO','SO2','O3','Benzene','Toluene','Xylene','AQI'],outputCol="features")
        final= assembler.transform(df)
        final.select("features").show(truncate=False)
        print("Batch along with features")
        final.show()
        #lr= LogisticRegression(maxIter=10, regParam=0.01)
        #model= lr.fit(df)
        #paramMap= {lr.maxIter: 20}
        #paramMapCombined= paramMap.copy()
        #model_final= lr.fit(df, paramMapComabined)
        #print(model_final.extractParamMap())
        #prediction= model_final.transform(df)
        #print("fitted and trained and predicted")
        lr= LogisticRegression(featuresCol='features', labelCol='label', maxIter=100)
        #model= lr.fit(final)
        path="/opt/trained_python_model_LogisticRegression"
        model= LogisticRegressionModel.load(path=path).setPredictionCol("prediction")
        #print(model)
        #print("good mrng")
        #model_1=lr.fit(model)
        #predictions= model_1.transform(final)
        predictions= model.transform(final)
        print("Batch along with predictions")
        predictions.show()
        print("predicted values")
        predictions=predictions.select("prediction")
        predictions.show()
        #final_df = predictions.drop("features")
        #final_df.show()
        #print("complete")
        #assembler = VectorAssembler(inputCols=['PM2_5','PM10','NO','NO2','NOx','NH3','CO','SO2','O3','AQI'],outputCol="features")
        #final= assembler.transform(df)
        #final.select("features").show(truncate=False)
        #final.show()
        
        #model= lr.fit(final)
        #predictions= model.transform(final)
        #print("predicted values")
        #predictions.select("prediction").show()
        #final_df= predictions.drop("features")
        #final_df.show()
    ##tri=LogisticRegression(maxIter=10,
    ##                  regParam=0.01,
    ##                  featuresCol="features",
    ##                   labelCol="label")
    ##lr_model = tri.fit(spDF)
        #model = LogisticRegression(featuresCol= 'features', labelCol= 'label')
        #pipeline = Pipeline(stages= [model])
        #pipelineFit = pipeline.fit(final)
        #predictions= pipelineFit.transform(final)
        #print("predicted values")
        #predictions.select("prediction").show()
        
        #output= predictions.drop("features")
        #output.show()
    ##ssc = StreamingContext(spark, batchDuration= 3)
    ##ssc.writeStream.start()  
    ##ssc.awaitTermination()  
    

     #Convert timestamp field from string to Timestamp format
    #df_kafka_string_parsed_formatted_timestamped = df_kafka_string_parsed_formatted.withColumn("timestamp", to_timestamp(df_kafka_string_parsed_formatted.timestamp, 'yyyy-MM-dd HH:mm:ss'))

     #Print schema information as a header
    #df_kafka_string_parsed_formatted_timestamped.printSchema()

    # Compute average of speed, accelerometer_x, accelerometer_y, and accelerometer_z during 5 minutes
    # Data comes after 10 minutes will be ignores
    #df_windowavg = df_kafka_string_parsed_formatted_timestamped.withWatermark("timestamp", "10 minutes").groupBy(
       # window(df_kafka_string_parsed_formatted_timestamped.timestamp, "5 minutes"),
       # df_kafka_string_parsed_formatted_timestamped.device_id).avg("speed", "accelerometer_x", "accelerometer_y", "accelerometer_z")

    # Add columns showing each window start and end timestamp
    #df_windowavg_timewindow = df_windowavg.select(
        #"device_id",
       # col("window.start").alias("window_start"),
       # col("window.end").alias("window_end"),
       # col("avg(speed)").alias("avg_speed"),
       # col("avg(accelerometer_x)").alias("avg_accelerometer_x"),
       # col("avg(accelerometer_y)").alias("avg_accelerometer_y"),
       # col("avg(accelerometer_z)").alias("avg_accelerometer_z")
       # ).orderBy(asc("device_id"), asc("window_start"))

    # Print output to console
    #query= df_kafka_string_parsed_formatted.foreach(get_prediction)
    process_df= df_kafka_string_parsed_formatted.writeStream.foreachBatch(get_prediction).start()
    #query_console= df_kafka_string_parsed_formatted.writeStream.outputMode("update").format("console").start() #query_console=foreach(get_prediction(df_kafka_string_parsed_formatted)).writeStream.start()
    #query_console = df_kafka_string_parsed_formatted.foreach(get_prediction).writeStream.outputMode("update").format("console").start()
    #query_console.awaitTermination()
    process_df.awaitTermination()
    spark.stop()