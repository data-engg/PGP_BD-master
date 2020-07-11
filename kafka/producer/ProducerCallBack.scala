import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class ProducerCallBack extends Callback{

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null){
      exception.printStackTrace()
    } else {
      println(println("message published to topic : " + metadata.topic() + "to partition : " + metadata.partition() + " offset : " + metadata.offset()))
    }
  }
}