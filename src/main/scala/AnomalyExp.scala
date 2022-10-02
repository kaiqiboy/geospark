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
import scala.collection.JavaConverters._

object AnomalyExp {
  case class E(shape: String, timeStamp: Array[Long], v: Option[String], d: String)

  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val numPartitions = args(1).toInt
    val queryFile = args(2)
    val threshold = args(3).split(",").map(_.toLong)
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkAnomalyExp")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)
    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(_.split(" "))
    val t = nanoTime
    for (q <- queries) {
      val pointDf = readEvent(dataFile, numPartitions)
      val pointRDD = Adapter.toSpatialRdd(pointDf, "location")
      pointRDD.analyze()
      pointRDD.buildIndex(IndexType.RTREE, false)
      val query = q.map(_.toDouble)
      val sQuery = new Envelope(query(0), query(2), query(1), query(3))
      val start = q(4).toLong
      val end = q(5).toLong
      val resultS = RangeQuery.SpatialRangeQuery(pointRDD, sQuery, true, true)
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .map {
          case (geoms, tsString) =>
            val timestamp = tsString.split("\t").head.toLong
            val id = tsString.split("\t").head
            (geoms, timestamp, id)
        }.filter { case (_, timestamp, _) =>
        timestamp < end && timestamp >= start
      }.filter {
        case (_, timestamp, _) => condition(getHour(timestamp))
      }
        .map(_._2)
      println(combinedRDD.collect.asScala.toArray.take(5).deep)
      combinedRDD.unpersist()
      pointRDD.rawSpatialRDD.unpersist()
      spark.catalog.clearCache()
    }
    println(s"Anomaly ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def readEvent(file: String, numPartitions: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val readDs = spark.read.parquet(file)
    readDs.createOrReplaceTempView("input")
    val sqlQuery = "SELECT ST_GeomFromWKT(input.shape) AS location, " +
      "CAST(element_at(input.timeStamp, 1) AS STRING) AS timestamp, " +
      "input.d AS id FROM input"
    val pointDF = spark.sql(sqlQuery)
    pointDF //.repartition(numPartitions)
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