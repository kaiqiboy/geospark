import TrajRangeQuery.T
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import scala.collection.JavaConverters._
import java.lang.System.nanoTime
import scala.io.Source
import scala.math.{abs, acos, cos, sin}

object AvgSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
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
        val coords = geoms.getCoordinates.map(x => (x.x, x.y))
        val length = coords.sliding(2).map(x => greatCircleDistance(x(0), x(1))).sum
        (id, length / (timestamps.last - timestamps.head) * 3.6)
      }
      println(combinedRDD.collect.asScala.toArray.take(5).deep)
      spark.catalog.clearCache()
    }
    println(s"avg speed ${(nanoTime - t) * 1e-9} s")
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
