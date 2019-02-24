import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val list = List(new Person("yms","man"),new Person("yms","man"),new Person("yms","man"))

    val conf = new SparkConf().setAppName("ProvinceJobNum").setMaster("local[5]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(list)



  }
}
