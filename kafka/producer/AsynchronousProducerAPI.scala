import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.{AuthenticationException, SerializationException, TimeoutException}
object AsynchronousProducerAPI {

  def main (args : Array[String]) = {
    val kafkaProps : Properties = new Properties()
    kafkaProps.put("bootstrap.servers", "ip-20-0-31-210.ec2.internal:9092")
    kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", "1")
    kafkaProps.put("retries", "0")

    val producer = new KafkaProducer[String, String](kafkaProps)

    for ( i <- 0 to 100){
      try{
        var record  = new ProducerRecord("edu_735821_kafka", "key" + i, "Meassage number : " + i)
        producer.send(record, new ProducerCallBack)
        Thread.sleep(1000)

      } catch {

        case ae : AuthenticationException => {
          println("Authentication failed : " + ae.printStackTrace())
        }

        case se : SerializationException => {
          print("Serilisation exception" + se.printStackTrace())
        }

        case te : TimeoutException =>{
          println("Time taken for fetching metadata or allocating memory for the record has surpassed max.block.ms." + te.printStackTrace())
        }

        case e : Exception => {
          e.printStackTrace()
        }
      } finally {
        println("End of code execution")
      }

    }
  }

}
