import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by prane on 9/7/2016.
  */
object SparkEvenNumberGenerator {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input.txt")

    //this takes the input file and goes line by line and splits the tokens using " "(space) as a delimiter.
    //It also filters out the empty tokens

    val fil_input = input.flatMap((line=>{line.split(" ").filter(_.nonEmpty)}))

    //In this step the filtering takes place such that only the words with even length are stored in our main_filter

    val main_filter = (fil_input.filter(e => e.stripPrefix(",").stripSuffix(",").trim.length%2==0))

    //this strips of the comma or period appended/prepended to the output

    val stripped_data = main_filter.map(e => e.stripPrefix(",").stripSuffix(",").stripPrefix(".").stripSuffix(".").trim)

    //in order to view the content of the RDD, in other words to output on the console we use collect()

    val console_data = stripped_data.collect()

    //this saves the output in the output folder in part files.

    stripped_data.saveAsTextFile("output")

    //printing the even length words to the console

    println(console_data.mkString("\n"))

  }

}
