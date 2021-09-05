import TrajRangeQuery.readTraj
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import scala.math.{abs, acos, cos, sin}

object AvgSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val numPartitions = args(1).toInt

    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkAvgSpeed")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajDf = readTraj(dataFile, numPartitions)
    val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
    trajRDD.rawSpatialRDD.rdd.cache()
    println(trajRDD.rawSpatialRDD.count())
    val t = nanoTime
    val combinedRDD = trajRDD.rawSpatialRDD.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
      .map {
        case (geoms, tsString) =>
          val timestamps = tsString.split("\t").last.split(",").map(_.toLong)
          val coords = geoms.getCoordinates.map(x => (x.x, x.y))
          val length = coords.sliding(2).map(x => greatCircleDistance(x(0), x(1))).sum
          length / (timestamps.last - timestamps.head) * 3.6
      }
    println(combinedRDD.take(5))
    println(s"Avg speed ${(nanoTime - t) * 1e-9} s" )

    sc.stop()
  }

  def greatCircleDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
    val x1 = p1._1
    val x2 = p2._1
    val y1 = p1._2
    val y2 = p2._2
    val r = 6371009 // earth radius in meter
    val phi1 = y1.toRadians
    val lambda1 = x1.toRadians
    val phi2 = y2.toRadians
    val lambda2 = x2.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }
}
