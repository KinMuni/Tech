import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.examples.wordcount.util.WordCount
import org.apache.spark.sql.execution.streaming.StreamExecution

object WordCount {


  def main(args: Array[String]){
    val params = ParameterTool.fromArgs(args)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)


    val text =

      if(params.has("input")) {
        env.readTextFile(params.get("input"))
      } else{
        print("Executing wordCount example with default input data set")
        print("use --input to specify file input")

        env.fromElements(WordCountData.WORDS:_*)
      }
    val counts: DataStream(String, Int) = text
      //

      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))

      .keyBy(0)
      .sum(1)

    if (params.has("output")) {
      counts.writeAsText(params.get("put.put"))
    } else {
      println("Printing result to status stdout. Use --output to specify output path")
      counts.print()
    }

    env.execute("Streaming WordCount")
  }


}
