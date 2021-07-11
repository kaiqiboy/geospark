import scala.io.Source
object ReadQueries {
  def readQueries(filename: String = "datasets/hzqueries.txt"): Array[Array[Double]] = {
    var res = new Array[Array[Double]](0)
    for (line <- Source.fromFile(filename).getLines) {
      val query = line.split(" ").map(_.toDouble)
      res = res :+ query
    }
    res
  }
}