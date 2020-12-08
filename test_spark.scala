import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object SparkWordCount {
   def main(args: Array[String]) {

    val sc = new SparkContext( "local", "Word Count", "/opt/spark", Nil, Map(), Map())           
      val input = sc.textFile("/opt/spark/test_data/input.txt")             
      val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)       
      count.saveAsTextFile("/opt/spark/test_data/outfile")
      System.out.println("OK");
   }
}


