package com.producer

import java.util.Properties
import com.google.gson.Gson
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.Callback
import java.util.Calendar
import java.util.Date

class MyProducerCallBack extends Callback{
  @Override
  def onCompletion(recordMetadata : RecordMetadata, exception : Exception) : Unit = {
    if(exception != null){
      println("Asynchronous Producer failed with exceptions!")
      exception.printStackTrace()
    }
    else{
//      println("Asynchronous Producer succeeded!")
    }
  }
}

class GetProducerAPI {
	val props = new Properties()
	props.put("bootstrap.servers", "localhost:9092,localhost:9093")
	props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	
	// Simple Producer - Fire and Forget
	def sendViaSimpleProducer(topicName : String, dataList : Array[String]) : Boolean = {
    val props = this.props
	  var flag : Boolean = true
	  val producer : Producer[String, String] = new KafkaProducer(props);

    try{
	    for(jsonData <- dataList){
  	    val record = new ProducerRecord[String, String](topicName, jsonData)
  	    producer.send(record)
  	  }
	  }
	  catch{
	    case e : Exception => {
	        flag = false
	        e.printStackTrace()
	    }
	  }
	  finally{
	    producer.close()
	  }
	  flag
	}
	
	// Synchronous Producer - Waits for the Acknowledgement
	def sendViaSynchronousProducer(topicName : String, dataList : Array[String]) : Boolean = {
    val props = this.props
	  var flag : Boolean = true
	  val producer : Producer[String, String] = new KafkaProducer(props);

    try{
	    for(jsonData <- dataList){
  	    val record = new ProducerRecord[String, String](topicName, jsonData)
  	    val metadata : RecordMetadata = producer.send(record).get()
  	    // println("Data sent to Partition Number: " + metadata.partition() + " and Offset: " + metadata.offset() )
  	  }
	  }
	  catch{
	    case e : Exception => {
	        flag = false
	        e.printStackTrace()
	    }
	  }
	  finally{
	    producer.close()
	  }
	  flag
	}
	
	// Asynchronous Producer - Doesn't wait for Acknowledgement, retries only for failures.
	def sendViaAsynchronousProducer(topicName : String, dataList : Array[String]) : Boolean = {
    val props = this.props
    props.put("max.in.flight.requests.per.connection", "25")
    
	  var flag : Boolean = true
	  val producer : Producer[String, String] = new KafkaProducer(props);

    try{
	    for(jsonData <- dataList){
  	    val record = new ProducerRecord[String, String](topicName, jsonData)
  	    producer.send(record, new MyProducerCallBack())
  	  }
	  }
	  catch{
	    case e : Exception => {
	        flag = false
	        e.printStackTrace()
	    }
	  }
	  finally{
	    producer.close()
	  }
	  flag
	}	
	
}