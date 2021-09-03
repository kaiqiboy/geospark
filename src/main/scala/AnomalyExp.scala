import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source

object AnomalyExp {
  case class E2(id: String, lon: Double, lat: Double, t: Long)

  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val threshold = args(3).split(",").map(_.toLong)
    val f = Source.fromFile(queryFile)

    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkAnomalyExp")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val pointDf = readEvent(dataFile, numPartitions)
    val pointRDD = Adapter.toSpatialRdd(pointDf, "location")
    pointRDD.analyze()
    pointRDD.buildIndex(IndexType.RTREE, false)
    pointRDD.indexedRawRDD.rdd.cache()
    println(pointRDD.rawSpatialRDD.count())
    val t = nanoTime

    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)

    val combinedRDD = pointRDD.rawSpatialRDD.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
      .map {
        case (geoms, tsString) =>
          val timestamp = tsString.split("\t").head.toLong
          (geoms, timestamp)
      }.rdd
      .filter {
        case (_, timestamp) => condition(getHour(timestamp))
      }
      .map(x => (longToWeek(x._2), 1))
      .reduceByKey(_+_)
    println(combinedRDD.collect.toMap)
    println(s"Range querying ${(nanoTime - t) * 1e-9} s")

    sc.stop()
  }

  def readEvent(file: String, numPartitions: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val readDs = spark.read.parquet(file)
    import spark.implicits._
    val pointRDD = readDs.as[E2].rdd.map(e => {
      val coord = s"${e.lon},${e.lat}"
      (coord, e.id, e.t)
    })
    val df = pointRDD.toDF("coord", "id", "t")
    df.createOrReplaceTempView("input")
    val sqlQuery = "SELECT ST_PointFromText(input.coord, ',') AS location, " +
      "CAST(input.t AS STRING) AS timestamp, " +
      "input.id AS id FROM input"
    val pointDF = spark.sql(sqlQuery)
    pointDF.repartition(numPartitions)
  }

  def longToWeek(t: Long): Int = {
    val d = new Date(t * 1000)
    val formatter = new SimpleDateFormat("w")
    val week = Integer.parseInt(formatter.format(d))
    week
  }
  def getHour(t: Long): Int =
    timeLong2String(t).split(" ")(1).split(":")(0).toInt

  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }
}