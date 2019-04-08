import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType,StructField,StringType}
object Events_Data {
def main(args: Array[String]): Unit = {
    // get csv file
    val spark = SparkSession.builder().appName("Events_Data").master("local").enableHiveSupport().getOrCreate()

    val events_input = "hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_input/events.csv"
   val events_schema = StructType(List(StructField("patient_id",StringType,false), StructField("event_id",StringType,false), StructField("event_description",StringType,true), StructField("timestamp",StringType,true), StructField("value",StringType,true)
    ))

    val EventsDF = spark.read.format("csv").schema(events_schema).load(events_input)
    EventsDF.toDF().createOrReplaceTempView("events")
    spark.sqlContext.sql("DROP TABLE IF EXISTS Events_Results")
    spark.sqlContext.sql("CREATE TABLE Events_Results AS SELECT * FROM events")

  }
}
