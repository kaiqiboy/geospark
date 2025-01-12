import SmSpeed.greatCircleDistance
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime

object RasterSpeedExtraction {
  case class TrajPoint(lon: Double, lat: Double, t: Array[Long], v: Option[String])

  case class T(points: Array[TrajPoint], d: String)

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
      val resRDD = combinedRDD
        .map {
          case (geoms, timestamps) =>
            val coords = geoms.getCoordinates.map(x => (x.x, x.y))
            val length = coords.sliding(2).map(x => greatCircleDistance(x(0), x(1))).sum
            val speed = (length / (timestamps.last - timestamps.head) * 3.6).toDouble
            raster.map {
              case (s, t) => if (s.intersects(geoms) && tIntersects(t, (timestamps.head, timestamps.last))) (speed,1) else (0.0,0)
            }
        }
      val r = resRDD.mapPartitions { p =>
        var res = raster.map(_ => (0.0, 0))
        while (p.hasNext) {
          res = res.zip(p.next).map { case (x, y) => (x._1 + y._1, x._2 + y._2) }
        }
        Iterator(res)
      }.collect()

      val res = r.drop(1).foldLeft(r.head)((a, b) => a.zip(b).map { case (x, y) => (x._1 + y._1, x._2 + y._2) })
        .map(x => x._1 / x._2)

      println(res.take(10).deep)
      spark.catalog.clearCache()
      sc.getPersistentRDDs.foreach(x => x._2.unpersist())
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

  def readTraj(file: String, numPartitions: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val readDs = spark.read.parquet(file)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(t => {
      val string = t.points.flatMap(e => Array(e.lon, e.lat)).mkString(",")
      val tsArray = t.points.flatMap(e => Array(e.t(0))).mkString(",")
      (string, t.d, tsArray)
    })
    val df = trajRDD.toDF("string", "id", "tsArray")
    df.createOrReplaceTempView("input")
    val sqlQuery = "SELECT ST_LineStringFromText(CAST(input.string AS STRING), ',') AS linestring, " +
      "CAST(input.id AS STRING) AS id," +
      "CAST(input.tsArray AS STRING)  AS tsArray " +
      "FROM input"
    val lineStringDF = spark.sql(sqlQuery)
    lineStringDF.repartition(numPartitions)
  }
}
