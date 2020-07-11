name := "MidProject"
version := "0.1"
scalaVersion := "2.10.4"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += "Apache Repository" at "https://repository.apache.org/content/repositories/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.15.1"
libraryDependencies += "com.ngdata" % "hbase-indexer-mr" % "1.5-cdh5.15.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.0-cdh5.15.1" % "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.8.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.8.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.8.0"
