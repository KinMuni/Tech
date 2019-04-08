import org.apache.spark._
import org.apache.spark.sql._

object batch_processing_final {

  def main(args:Array[String]): Unit = {

    // Do : Spark Configuration and Define SC variable
    val conf
    val sc

    // Do : Define SqlContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Getting Data to Data Frame Directly
    val event_data_csv =
    val mortality_data_csv =

    /**
      * Event Count : Number of events recorded for a given patient.
      * Note that every line in the input file is an event.
      * Return: avg, min, max count for alive patient and dead patient
      * Save the result to HDFS "../maria_dev/med_output"
      * */


  }

}
