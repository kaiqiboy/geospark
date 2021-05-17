import org.apache.spark.sql.{Dataset, SparkSession}
import com.vividsolutions.jts.geom.Envelope

import scala.io.Source


object ReadQueryFile extends Serializable {

  def apply(f: String): Array[Envelope] = {
    var queries = new Array[Envelope](0)
    for (line <- Source.fromFile(f).getLines) {
      val r = line.split(" ")
      queries = queries :+ new Envelope(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble)
    }
    queries
  }
}
