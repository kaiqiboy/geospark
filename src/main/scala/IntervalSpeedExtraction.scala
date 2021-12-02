import TrajRangeQuery.readTraj
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import scala.collection.JavaConverters._
import scala.io.Source
import scala.math.{abs, acos, cos, sin}

object IntervalSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val sQuery = args(2).split(",").map(_.toDouble)
    val tStart = args(3).toLong
    val NumDays = args(4).toInt
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("IntervalSpeed")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ranges = (0 to NumDays).map(x =>
      (sQuery, (x * 86400 + tStart, (x + 1) * 86400 + tStart))).toArray
    val t = nanoTime
    for ((s, tQuery) <- ranges) {
      val trajDf = readTraj(fileName, numPartitions)
      val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
      trajRDD.analyze()
      trajRDD.buildIndex(IndexType.RTREE, false)
      val sQuery = new Envelope(s(0), s(2), s(1), s(3))
      val start = tQuery._1
      val end = tQuery._2
      val resultS = RangeQuery.SpatialRangeQuery(trajRDD, sQuery, true, true)
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .map {
          case (geoms, tsString) =>
            val timestamps = tsString.split("\t").last.split(",").map(_.toLong)
            val id = tsString.split("\t").head
            (geoms, timestamps, id)
        }
        .filter { case (_, timestamps, _) =>
          timestamps.head < end && timestamps.last >= start
        }.map { case (geoms, timestamps, id) =>
        val coords = geoms.getCoordinates.map(x => (x.x, x.y))
        val points = (coords zip timestamps)
        val lengths = coords.sliding(2).map(x => greatCircleDistance(x(0), x(1))).toArray
        val durations = timestamps.sliding(2).map(x => x(1) - x(0)).toArray
        val speeds = (lengths zip durations).map(x => x._1 / x._2 * 3.6)
        (id, points zip speeds)
      }
      combinedRDD.collect.asScala.toArray.take(5).foreach(x => println(x._1, x._2.deep))
      spark.catalog.clearCache()
    }
    println(s"Internal speed ${(nanoTime - t) * 1e-9} s")
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
