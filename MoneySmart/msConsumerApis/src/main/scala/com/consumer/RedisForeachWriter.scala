package com.consumer

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import org.apache.spark.sql.Dataset

class RedisForeachWriter(val host: String, port: String, val topic: String) extends ForeachWriter[Row]{
    // val host: String = p_host
    // val port: String = p_port

    var jedis: Jedis = _

    def connect() = {
        jedis = new Jedis(host, port.toInt)
    }

    override def open(partitionId: Long, version: Long): Boolean = {
        return true
    }

    override def process(record: Row) = {
        val u_id = record.getString(1);
        
        if( !(u_id == null || u_id.isEmpty())){
          val columns : Array[String] = record.schema.fieldNames
  
          if(jedis == null){
              connect()
          }
          
          for(i <- 0 until columns.length){
            if(! ((record.getString(i) == null) || (record.getString(i).isEmpty()) || record.getString(i) == "") )
              jedis.hset(s"${topic}:" + u_id, columns(i), record.getString(i))
          }
        }
    }

    override def close(errorOrNull: Throwable) = {
    
    }
}