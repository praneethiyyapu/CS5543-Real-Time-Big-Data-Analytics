import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by prane on 8/30/2016.
  */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input.txt")

    val wc=input.flatMap(line=>{line.split("\\.\\ ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_).sortByKey()

    output.saveAsTextFile("output")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
