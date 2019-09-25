package com.consumer

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

import com.redislabs.provider.redis
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.RedisContext
import com.redislabs.provider.redis.rdd._
import com.google.gson.Gson
import sun.security.pkcs11.Session
import java.util.Date


object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val topic = if(args.isEmpty || args(0) == "") "MS" else args(0)
    println(s"\n\n>>>> Topic is: ${topic}!\n\n")

    // SPARKCONF WITH REDIS, ELASTICSEARCH & CASSANDRA CONFIGURATIONS
    val conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkStreamingConsumerApp")
                .set("spark.sql.shuffle.partition", "2")
                .set("es.index.auto.create", "true")        // Elasticsearch configs
                .set("es.nodes", "http://localhost:9200")   // Elasticsearch configs
                .set("es.nodes.discovery", "false")         // Elasticsearch configs
                .set("es.nodes.data.only", "false")         // Elasticsearch configs
                .set("spark.cassandra.connection.host", "127.0.0.1")      // Cassandra configs
                .set("spark.cassandra.connection.port", "9042")           // Cassandra configs
                .set("spark.cassandra.connection.keep_alive_ms", "60000") // Cassandra configs
                .set("spark.redis.host", "localhost")    // Redis configs
                .set("spark.redis.port", "6379")         // Redis configs
    
    // CREATE SPARK SESSION OBJECT
    val spark = SparkSession.builder()
                        .config(conf)
                        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    spark.sql("set spark.sql.shuffle.partitions=2")

    // CASSANDRA CONNECTOR CONFIGURATION - FOREACH SINK
    val cassandraKeyspaceRaw = "test"
    val cassandraTableRaw = "moneysmart"
    val cassandraTableProcessed = "moneysmartprocessed"
    // This Cassandra ForeachWriter is for raw input
    // val cassandraForeachWriterRaw = new CassandraSinkForeachRaw(spark.sparkContext.getConf, cassandraKeyspaceRaw, cassandraTableRaw)
    
    // This Cassandra ForeachWriter is for processed value
    val cassandraForeachWriterProcessed = new CassandraSinkForeachProcessed(spark.sparkContext.getConf, cassandraKeyspaceRaw, cassandraTableProcessed)

    // REDIS CONNECTOR - FOREACHWRITER SINK
    val redisForeachWriter : RedisForeachWriter = new RedisForeachWriter("localhost","6379", topic)
    
    // UDF FOR SESSION ID CREATION
    val newSessionIdUDF = udf[String, String]((userId: String) => { createNewSessionId(userId) } )
      
    
    // SCHEMA FOR KAFKA STREAM - "BRUTE FORCE" APPROACH. SHOULD BE HANDLED USING AVRO/PARQUET IN PRODUCTION.
    val kafkaStreamSchema = spark.read
                                .format("json")
                                .option("path", "/home/kr_stevejobs/projects/moneysmart/sampledata/clickstream-sample.json")
                                .option("inferSchema", "true")
                                .load()
                                .schema
    
    // READ INCOMING KAFKA STREAM
    val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .load
        .selectExpr("CAST(NOW() as LONG) AS TS", "CAST(value AS STRING)")
    
    // EXCTRACT THE DATA FROM RAW KAFKA STREAM DATA
    val dataDf = df.selectExpr("ts","CAST(value AS STRING) AS json")
                   .select(from_json(col("json"), kafkaStreamSchema).as("data"))
                   .select("data.*")
                   .selectExpr("date AS message_date", "*")
                   .select("user_id", "message_date", "user_agent", "partner_id", "partner_name", "init_session", 
                      "session_id", "page_type", "category", "cart_amount", "platform"
                    )
                   .selectExpr("CAST(NOW() AS LONG) AS ts", "*")
                   .withColumn("ts", col("ts").cast(StringType))
                   .withColumn(     
                        // Assuming there are two platforms - MOBILE & WEB
                        "platform",
                        when(col("platform") === "MOBILE", lit("MOBILE")).otherwise(lit("WEB"))
                    )
                    .withColumn(    
                        // Identify the device
                        "device",
                        when(
                            (col("user_agent").like("%Windows%")), lit("Windows PC")
                        ).when(
                            (col("user_agent").like("%android%")), lit("Android")
                        ).when(
                            (col("user_agent").like("%iPhone%")), lit("iPhone")
                        ).when(
                            (col("user_agent").like("%iPad%")), lit("iPad")
                        ).when(
                            (col("user_agent").like("%Mac%")), lit("iMac")
                        ).otherwise(lit("Others"))
                    )
                    .withColumn(    
                        // Update the last visit details
                        "last_visited", 
                        col("message_date")
                    )
    
    // SCHEMA FOR STATE DATA CACHED IN REDIS DATA
    val redisSchema = StructType(
        List(
            StructField("tstamp",StringType,true),
            StructField("u_id",StringType,true),
            StructField("msg_date",StringType,true),
            StructField("usr_agent",StringType,true),
            StructField("prtnr_id",StringType,true),
            StructField("prtnr_name",StringType,true),
            StructField("sess_id",StringType,true),
            StructField("pg_type",StringType,true),
            StructField("ctgry",StringType,true),
            StructField("cart_amt",StringType,true),
            StructField("pltfrm",StringType,true),
            StructField("last_visit",StringType,true),
            StructField("user_device",StringType,true)
        )
    )
    
    // EXTRACTING PREVIOUS STATE OF USERS FROM REDIS
    val keysPattern = s"${topic}:*"
    
    val redisDf = spark.read
                    .format("org.apache.spark.sql.redis")
                    .schema(redisSchema)
                    .option("keys.pattern", keysPattern)
                    .load
    
    // JOIN THE STREAMING DATA WITH PREVIOUS STATE DATA FOR COMPARISON
    val joinedDf = dataDf.joinWith(
                          redisDf,
                          trim(col("user_id")) === trim(col("u_id")),
                          "left"
                        ).select("_1.*", "_2.*")
    
    // FINDING THE NEW USER RECORDS
    val newDf = joinedDf.filter(col("user_id").isNotNull && col("u_id").isNull)
                        .withColumn(
                            // Fix session_id if not available
                            "session_id", 
                            when(
                                (col("init_session") === true || col("session_id").isNull),
                                newSessionIdUDF(col("user_id"))
                            ).otherwise(col("session_id"))
                        )
                        .withColumn("tstamp",col("ts"))
                        .withColumn("u_id", col("user_id"))
                        .withColumn("msg_date", col("message_date"))
                        .withColumn("prtnr_id", col("partner_id"))
                        .withColumn("prtnr_name", col("partner_name"))
                        .withColumn("sess_is", col("session_id"))
                        .withColumn("pg_type", col("page_type"))
                        .withColumn("ctgry", col("category"))
                        .withColumn("cart_amt", lit("0"))  // previous cart_amount of user is 0 as this is his/her first record.
                        .withColumn("pltfrm", col("platform"))
                        .withColumn("last_visited", col("last_visit"))
                        .withColumn("user_device", col("device"))
    
    // FINDING UPDATES IN USER RECORDS WITH RESPECT TO PREVIOUS STATE OF CUSTOMERS
    val updatesDf = joinedDf.filter(col("user_id") === col("u_id"))
                    .withColumn(
                      "session_id",
                      when(
                          // If difference of timestamp > 30 min or init == true, create new session
                          ((col("message_date") - col("msg_date"))/60 > 30) || (col("init_session") === true), 
                          newSessionIdUDF(col("user_id")) 
                      ).otherwise(col("session_id"))
                    )
                    .withColumn(
                      // partner_id change detection
                      "prtnr_id",    
                      when(
                          (col("partner_id") !== col("prtnr_id")),
                          col("partner_id")
                      ).otherwise(col("prtnr_id"))
                    )
                    .withColumn(
                      // partner_name change detection
                      "prtnr_name",  
                      when(
                          (col("partner_name") !== col("prtnr_name")),
                          col("partner_name")
                      ).otherwise(col("prtnr_name"))
                    )
                    .withColumn(
                      // last cart_amount of previous purchase
                      "cart_amt",  
                      when(
                          (col("page_type").like("success")),
                          col("cart_amount")
                      ).otherwise(col("cart_amt"))
                    )
                    .withColumn(
                      // category of previous purchase
                      "ctgry",  
                      when(
                          (col("page_type").like("success")),
                          col("category")
                      ).otherwise(col("ctgry"))
                    )
                    .withColumn("pg_type", col("page_type"))
                    .withColumn("pltfrm", col("platform"))
                    .withColumn("user_device", col("device"))
                    .withColumn("tstamp",col("ts"))
    
    // CONSOLE WRITER - FOR DEV ONLY - TO CHECK INTERMEDIATE RESULTS
    val q2 = joinedDf.writeStream
              .format("console")
              .outputMode("update")
              .start
    
    // PUSH NEW USER DETAILS TO REDIS FOR STATE REFERENCE
    val rnq = newDf
                 .select(
                     "tstamp", "u_id", "msg_date", "usr_agent", "prtnr_id", "prtnr_name", 
                     "sess_id", "pg_type", "ctgry", "cart_amt", "pltfrm", "last_visit", "device"
                 )
                 .writeStream
                 .outputMode("update")
                 .foreach(redisForeachWriter)
                 .start

    // PUSH UPDATED USER DETAILS TO REDIS FOR STATE REFERENCE
    val ruq = updatesDf
                 .select(
                     "tstamp", "u_id", "msg_date", "usr_agent", "prtnr_id", "prtnr_name", 
                     "sess_id", "pg_type", "ctgry", "cart_amt", "pltfrm", "last_visit", "device"
                 )
                 .writeStream
                 .outputMode("update")
                 .foreach(redisForeachWriter)
                 .start
    
    // WRITE NEW USER RECORDS IN CASSANDRA
    val cnq = newDf.select(
                 "ts", "user_id", "cart_amount", "category", "init_session", "last_visited", "message_date", 
                 "page_type", "partner_id", "partner_name", "platform", "session_id", "user_agent", "user_device"
               )
               .writeStream
               .outputMode("update")
               .foreach(cassandraForeachWriterProcessed)
               .start

    // WRITE USER UPDATE RECORDS IN CASSANDRA
    val cuq = updatesDf.select(
                 "ts", "user_id", "cart_amount", "category", "init_session", "last_visited", "message_date", 
                 "page_type", "partner_id", "partner_name", "platform", "session_id", "user_agent", "user_device"
               )
               .writeStream
               .outputMode("update")
               .foreach(cassandraForeachWriterProcessed)
               .start
    
    q2.awaitTermination
    rnq.awaitTermination
    ruq.awaitTermination
    cnq.awaitTermination
    cuq.awaitTermination
    
  }
  
  def createNewSessionId(userId: String) : String = {
    var ts : Long = new Date().getTime
    var newSessionId = ""
    if(! (userId == null || userId.isEmpty)){
        newSessionId = s"${userId}-${ts}"
    }
    else{
      // Random session id generation
      val rnd = new scala.util.Random
      val randomNumber = 10000 + rnd.nextInt( (999999 - 10000) + 1 )  
      newSessionId = s"${randomNumber}"
    }
    newSessionId
  }
}
