import java.util.{Properties,Collections}
import org.apache.kafka.clients.consumer.KafkaConsumer

object SimpleConsumerAPI {

  def main ( args : Array[String]){
    val kafkaProps : Properties = new Properties()
    kafkaProps.put("bootstrap.servers","ip-20-0-31-210.ec2.internal:9092")
    kafkaProps.put("group.id", "edu_grp_01")
    kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer : KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProps)
    consumer.subscribe(Collections.singletonList("edu_735821_kafka"))

    while (true){
      try{

        val records = consumer.poll(1000)
        print(records)
        System.exit(1)
        /*
        val record_itr = records.iterator()
        var record = record_itr.next()
        while ( record != null){
          println("partition id : " +record.partition() +
                  " , record : " + record.offset() +
                  " , key : " + record.key() +
                  " , value" + record.value())

          record = record_itr.next()
        }*/
      } catch {
        case e: Exception => {
          e.printStackTrace()
          System.exit(1)
        }
      } finally {
        consumer.close()
      }
    }
  }
}
