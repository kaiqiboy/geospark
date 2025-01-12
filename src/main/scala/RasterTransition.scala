import TrajRangeQuery.T
import com.vividsolutions.jts.geom._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source
import scala.math.{abs, acos, cos, sin}

object RasterTransition {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val sSplit = args(2).toDouble
    val tSplit = args(3).toInt
    val numPartitions = args(4).toInt
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
    val geometryFactory = new GeometryFactory()

    for (q <- queries) {
      val sRanges = splitSpatial(Array(q(0), q(1), q(2), q(3)).map(_.toDouble), sSplit)
      val tRanges = splitTemporal(Array(q(4), q(5)).map(_.toLong), tSplit)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val trajDf = readTraj(dataFile, numPartitions)
      val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
      trajRDD.analyze()
      trajRDD.buildIndex(IndexType.RTREE, false)
      val query = q.map(_.toDouble)
      val sQuery = new Envelope(query(0), query(2), query(1), query(3))
      val start = q(4).toLong
      val end = q(5).toLong
      val resultS = RangeQuery.SpatialRangeQuery(trajRDD, sQuery, true, true)
      trajRDD.indexedRawRDD.unpersist()
      trajRDD.rawSpatialRDD.unpersist()
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .map {
          case (geoms, tsString) =>
            val timestamps = tsString.split("\t").last.split(",").map(_.toLong)
            val id = tsString.split("\t").head
            (geoms, timestamps)
        }.filter { case (_, timestamps) =>
        timestamps.head < end && timestamps.last >= start
      }.rdd
      val resRDD = combinedRDD
        .map { case (linestring, timestamps) =>
          var in = 0
          var out = 0
          stRanges.map { stRange =>
            if (!(linestring.intersects(stRange._1) && tIntersects((timestamps.head, timestamps.last), stRange._2))) (0, 0)
            else {
              val points = linestring.getCoordinates.map(x => geometryFactory.createPoint(x)) zip timestamps
              val inside = points.map(p => p._1.intersects(stRange._1) &&
                stRange._2._1 <= p._2 && stRange._2._2 >= p._2).sliding(2)
              while (inside.hasNext) {
                val a = inside.next
                if (a == (true, false)) out += 1
                else if (a == (false, true)) in += 1
              }
              (in, out)
            }
          }
        }
      val empty = stRanges.map(_ => (0, 0))
      val res = resRDD.aggregate(empty)((x, y) => (x zip y).map(x => (x._1._1 + x._2._1, x._1._2 + x._2._2)),
        (x, y) => (x zip y).map(x => (x._1._1 + x._2._1, x._1._2 + x._2._2)))
      println(res.take(5))
      combinedRDD.unpersist()
      resRDD.unpersist()
      trajRDD.indexedRawRDD.unpersist()
      trajRDD.rawSpatialRDD.unpersist()
      spark.catalog.clearCache()
    }
    println(s"raster transition ${(nanoTime - t) * 1e-9} s")
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

  def splitTemporal(temporalRange: Array[Long], tStep: Int): Array[(Long, Long)] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tSplit = ((tMax - tMin) / tStep).toInt
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield (t(0), t(1))
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