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

object RasterFlowExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val sQuery = args(2).split(",").map(_.toDouble)
    val tStart = args(3).toLong
    val NumDays = args(4).toInt
    val tSplit = args(5).toInt
    val sSize = args(6).toInt
    val range = (sQuery(0), sQuery(1), sQuery(2), sQuery(3))
    val geometryFactory = new GeometryFactory()
    val ls = Array(new Coordinate(range._1, range._2),
      new Coordinate(range._1, range._4),
      new Coordinate(range._3, range._4),
      new Coordinate(range._3, range._2),
      new Coordinate(range._1, range._2))
    val sRange = geometryFactory.createPolygon(ls)
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("RasterFlow")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ranges = (0 to NumDays).map(x =>
      (sRange, (x * 86400 + tStart, (x + 1) * 86400 + tStart))).toArray
    for ((s, tQuery) <- ranges) {
      val t = nanoTime
      val raster = genRaster(tQuery._1, tQuery._2, s, tSplit, sSize)
      val trajDf = readTraj(fileName, numPartitions)
      val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
      trajRDD.analyze()
      trajRDD.buildIndex(IndexType.RTREE, false)
      val start = tQuery._1
      val end = tQuery._2
      val resultS = RangeQuery.SpatialRangeQuery(trajRDD, sRange, true, true)
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .map {
          case (geoms, tsString) =>
            val timestamps = tsString.split("\t").last.split(",").map(_.toLong)
            val id = tsString.split("\t").head
            (geoms, timestamps)
        }
        .filter { case (_, timestamps) =>
          timestamps.head < end && timestamps.last >= start
        }.rdd
      val resRDD = combinedRDD.map {
        x =>
          raster.map {
            case (s, t) => if (s.intersects(x._1) && tIntersects(t, (x._2.head, x._2.last))) 1 else 0
          }
      }
      val empty = raster.map(_ => 0)
      val res = resRDD.aggregate(empty)((x, y) => (x zip y).map(x => x._1 + x._2), (x, y) => (x zip y).map(x => x._1 + x._2))
      println(res.take(10).deep)
      spark.catalog.clearCache()
      println(s"${tQuery._1} Interval extraction ${(nanoTime - t) * 1e-9} s")
    }
    sc.stop()
  }

  def genRaster(startTime: Long, endTime: Long, sRange: Polygon,
                tSplit: Int, sSize: Int): Array[(Polygon, (Long, Long))] = {
    val durations = (startTime until endTime + 1 by tSplit).sliding(2).map(x => (x(0), x(1))).toArray
    val polygons = splitSpatial(sRange, sSize)
    for (s <- polygons; t <- durations) yield (s, t)
  }

  def splitSpatial(spatialRange: Polygon, gridSize: Int): Array[Polygon] = {
    val geometryFactory = new GeometryFactory()
    val xMin = spatialRange.getCoordinates.map(_.x).min
    val xMax = spatialRange.getCoordinates.map(_.x).max
    val yMin = spatialRange.getCoordinates.map(_.y).min
    val yMax = spatialRange.getCoordinates.map(_.y).max
    val xStep = (xMax - xMin) / gridSize
    val xs = (0 to gridSize).map(x => x * xStep + xMin).sliding(2).toArray
    val yStep = (yMax - yMin) / gridSize
    val ys = (0 to gridSize).map(y => y * yStep + yMin).sliding(2).toArray
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

  def tIntersects(t1: (Long, Long), t2: (Long, Long)): Boolean = {
    !(t1._2 < t2._1 || t2._2 < t1._1)
  }
}
