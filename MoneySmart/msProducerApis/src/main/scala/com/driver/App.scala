package com.driver

import com.producer.GetProducerAPI

import scala.io.Source

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.util.Timer
import java.util.TimerTask
import java.util.TimerTask
import java.util.Timer
import java.util.Date


object App { 
  var topic : String = null
  
  // Main function
  def main(args: Array[String]): Unit = {
    // Set the Kafka topic for the app
    topic = if(args.isEmpty || args(0) == "") "MS" else args(0)
    
    // Producer object
    val producer = new GetProducerAPI
    
    try{
      // Read data from Source (Text file in our example)
      val source = Source.fromFile("/home/kr_stevejobs/projects/moneysmart/sampledata/clickstream-sample.json")
      val lineIterator = source.getLines
      var counter = 1
      
      var dataList = new scala.collection.mutable.ArrayBuffer[String]
      
      for(line <- lineIterator){
        // println(s"Current Line ${counter} : ${line}")
        dataList += line
        counter += 1
        
        if (counter % 3 == 0){
          println("\n\nSending the current batch...")
          
          // Sending the messages to Kafka using AsynchronousProducer which retries only the failed messages.
          val status : Boolean = producer.sendViaAsynchronousProducer(this.topic, dataList.toArray)
          println(s"Sent status: ${if(status == true) "Successful!" else "Failed!"}")
          
          dataList.clear
          Thread.sleep(5000)
        }
      }
    }
    catch {
      case t: Throwable => {
        println("ERROR: Exception occured...\n\n")
        t.printStackTrace()
      }
    }
  }
}