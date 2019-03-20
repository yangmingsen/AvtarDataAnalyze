import java.io.FileWriter

import scala.io.Source

/**
  * program: AvtarDataAnalyze
  * author: ljq
  * create: 2019-03-20 16:51
  **/
object IOTest {
  private def FileConversion(): Unit = {
    val out = new FileWriter("src/main/scala/spark/rdd/ParticipleText/technology1")
    for (word <- Source.fromFile("src/main/scala/spark/rdd/ParticipleText/technology").getLines()) {
      word.split(",").foreach(x => out.write(x.toLowerCase() + ","))
    }
    out.close()
  }

  def main(args: Array[String]): Unit = {
    FileConversion()
  }
}
