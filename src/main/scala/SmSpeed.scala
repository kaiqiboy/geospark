import AvgSpeedExtraction.greatCircleDistance
import TrajRangeQuery.readTraj
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Polygon}
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

object SmSpeed {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val sSplit = args(2).toDouble
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkAvgSpeed")
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
      val ranges = splitSpatial(Array(q(0), q(1), q(2), q(3)).map(_.toDouble), sSplit)
      val trajDf = readTraj(dataFile, 2)
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
        }.rdd.flatMap { case (geoms, timestamps, _) =>
        val intersectCells = ranges.zipWithIndex.filter {
          case (rectangle, _) => rectangle.intersects(geoms)
        }.map(_._2)
        val coords = geoms.getCoordinates.map(x => (x.x, x.y))
        val length = coords.sliding(2).map(x => greatCircleDistance(x(0), x(1))).sum
        val speed = (length / (timestamps.last - timestamps.head) * 3.6)
        intersectCells.map(x => (x, (speed, 1)))
      }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2)
      println(combinedRDD.collect.take(5).deep)
      spark.catalog.clearCache()
    }
    println(s"Avg speed ${(nanoTime - t) * 1e-9} s")
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

  def splitSpatial(spatialRange: Array[Double], gridSize: Double): Array[Polygon] = {
    val geometryFactory = new GeometryFactory()
    val xMin = spatialRange(0)
    val yMin = spatialRange(1)
    val xMax = spatialRange(2)
    val yMax = spatialRange(3)
    val xSplit = ((xMax - xMin) / gridSize).toInt
    val xs = (0 to xSplit).map(x => x * gridSize + xMin).sliding(2).toArray
    val ySplit = ((yMax - yMin) / gridSize).toInt
    val ys = (0 to ySplit).map(y => y * gridSize + yMin).sliding(2).toArray
    for (x <- xs; y <- ys) yield {
      val range = (x(0), y(0), x(1), y(1))
      val ls = Array(new Coordinate(range._1, range._2),
        new Coordinate(range._1, range._4),
        new Coordinate(range._3, range._4),
        new Coordinate(range._3, range._2),
        new Coordinate(range._1, range._2))
      geometryFactory.createPolygon(ls)
    }
  }
}
