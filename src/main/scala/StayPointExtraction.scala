import TrajRangeQuery.readTraj
import com.vividsolutions.jts.geom.{Envelope, Geometry, LineString, Point}
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

object StayPointExtraction {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val maxDist = args(3).toDouble
    val minTime = args(4).toInt

    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkStayPoint")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(_.split(" "))
    val t = nanoTime
    for (q <- queries) {
      val trajDf = readTraj(dataFile, numPartitions)
      val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
      trajRDD.analyze()
      trajRDD.buildIndex(IndexType.RTREE, false)
      val query = q.map(_.toDouble)
      val sQuery = new Envelope(query(0), query(2), query(1), query(3))
      val start = q(4).toLong
      val end = q(5).toLong
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
        (id, findStayPoint(geoms.asInstanceOf[LineString], timestamps, maxDist, minTime))
      }
      println(combinedRDD.collect.asScala.toArray.take(5).deep)
      spark.catalog.clearCache()
    }
    println(s"stay point ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def findStayPoint(geoms: LineString, timestamps: Array[Long], maxDist: Double, minTime: Int): Array[(Double,Double)] = {
    val points = geoms.getCoordinates.map(x => (x.x, x.y))
    if (points.length < 2) return new Array[(Double,Double)](0)
    val entries = (points zip timestamps).toIterator
    var anchor = entries.next
    var res = new Array[(Double, Double)](0)
    var tmp = new Array[((Double, Double), Long)](0)
    while (entries.hasNext) {
      val candidate = entries.next
      if (greatCircleDistance((candidate._1._1, candidate._1._2), (anchor._1._1, anchor._1._2)) < maxDist) tmp = tmp :+ candidate
      else {
        if (tmp.length > 0 && tmp.last._2 - anchor._2 > minTime)
          res = res :+ anchor._1
        anchor = candidate
        tmp = new Array[((Double, Double), Long)](0)
      }
    }
    res
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
