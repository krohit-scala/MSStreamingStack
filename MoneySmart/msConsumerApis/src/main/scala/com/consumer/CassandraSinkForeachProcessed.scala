package com.consumer

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.QueryBuilder

class CassandraSinkForeachProcessed  (
    sparkConf: SparkConf, 
    val keyspace: String, 
    val tblName: String) extends ForeachWriter[Row] {
	
  def open(partitionId: Long, version: Long): Boolean = true

	def process(row: Row) = {
	  val columns : Array[String] = row.schema.fieldNames
	  
		def buildStatement: Statement = QueryBuilder
		                                  .insertInto(keyspace, tblName)
                                      .value("ts", row.getAs[String]("ts"))
                                      .value("user_id", row.getAs[String]("user_id"))
                                      .value("message_date", row.getAs[String]("message_date"))
                                      .value("user_agent", row.getAs[String]("user_agent"))
                                      .value("partner_id", row.getAs[String]("partner_id"))
                                      .value("partner_name", row.getAs[String]("partner_name"))
                                      .value("init_session", row.getAs[Boolean]("init_session"))
                                      .value("session_id", row.getAs[String]("session_id"))
                                      .value("page_type", row.getAs[String]("page_type"))
                                      .value("category", row.getAs[String]("category"))
                                      .value("cart_amount", row.getAs[String]("cart_amount"))
                                      .value("platform", row.getAs[String]("platform"))
                                      .value("user_device", row.getAs[String]("user_device"))
                                      
		CassandraConnector(sparkConf).withSessionDo { session => session.execute(buildStatement)}
	}

	def close(errorOrNull: Throwable) = Unit
}