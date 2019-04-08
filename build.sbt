name := "HBaseFinal"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "hortonworks" at "http://repo.hortonworks.com/content/groups/public/"


lazy val sparkVersion = "2.0.0"
lazy val kafkaVersion = "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % kafkaVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"
libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"

scalaVersion := "2.11.7"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
//Twitter 

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"