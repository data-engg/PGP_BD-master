import com.datastax.driver.core.{Session, Cluster}

class CassandraConnector {

  private var session : Session = null;
  private var cluster : Cluster = null;

  def this (node : String, port : Int, keyspace : String){
    this()
    this.cluster = Cluster.builder()
    .addContactPoint(node)
    .withPort(port)
    .withCredentials("***********", "****************")
    .build()

  this.session = cluster.connect(keyspace)
  }

   def getSession(): Session = {
      this.session
  }

  def close() : Unit = {
    this.session.close()
  }

}
