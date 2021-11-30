import SpeedGeoSpark.{Trajectory, calSpeed}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

case class Trajectory(points: Array[((Double, Double), Long)], id: String)

case class E(lon: Double, lat: Double, t: Long)

//case class T(id: String, entries: Array[E])

object TrajRangeQuery {
  //  def main(args: Array[String]): Unit = {
  //    val dataFile = args(0)
  //    val queryFile = args(1)
  //    val numPartitions = args(2).toInt
  //    val f = Source.fromFile(queryFile)
  //    val queries = f.getLines().toArray.map(line => {
  //      val r = line.split(" ")
  //      (r.take(4).map(_.toDouble), r.takeRight(2).map(_.toLong))
  //    })
  //
  //    var t = nanoTime
  //    val spark = SparkSession.builder()
  //      .master(Config.get("master"))
  //      .appName("GeoSparkRangeQueryTraj")
  //      .config("spark.serializer", classOf[KryoSerializer].getName)
  //      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  //      .getOrCreate()
  //
  //    GeoSparkSQLRegistrator.registerAll(spark)
  //    val sc = spark.sparkContext
  //    sc.setLogLevel("ERROR")
  //
  //    val trajDf = readTraj(dataFile, numPartitions)
  //    val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
  //    trajRDD.analyze()
  //    trajRDD.buildIndex(IndexType.RTREE, false)
  //    trajRDD.indexedRawRDD.rdd.cache()
  //    println(trajRDD.indexedRawRDD.count())
  //    println(s"Data loading ${(nanoTime - t) * 1e-9} s")
  //    t = nanoTime
  //    for (query <- queries) {
  //      val sQuery = new Envelope(query._1(0), query._1(2), query._1(1), query._1(3))
  //      val resultS = RangeQuery.SpatialRangeQuery(trajRDD, sQuery, true, true)
  //      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
  //        .map {
  //          case (geoms, tsString) =>
  //            val timestamps = tsString.split("\t").last.split(",").map(_.toLong)
  //            val id = tsString.split("\t")(0)
  //            (geoms, timestamps, id)
  //        }.rdd
  //        .filter {
  //          case (_, timestamps, _) => query._2(0) <= timestamps.last && query._2(1) >= timestamps.head
  //        }
  //      println(combinedRDD.count)
  //    }
  //    println(s"Range querying ${(nanoTime - t) * 1e-9} s")
  //
  //    sc.stop()


  case class TrajPoint(lon: Double, lat: Double, t: Array[Long], v: Option[String])

  case class T(points: Array[TrajPoint], d: String)

  def readTraj(file: String, numPartitions: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val readDs = spark.read.parquet(file)
    //    readDs.show(5)
    //    readDs.printSchema()
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(t => {
      val string = t.points.flatMap(e => Array(e.lon, e.lat)).mkString(",")
      val tsArray = t.points.flatMap(e => Array(e.t(0))).mkString(",")
      (string, t.d, tsArray)
    })
    val df = trajRDD.toDF("string", "id", "tsArray")
    df.createOrReplaceTempView("input")
    //    df.show(5)
    //    df.printSchema()
    val sqlQuery = "SELECT ST_LineStringFromText(CAST(input.string AS STRING), ',') AS linestring, " +
      "CAST(input.id AS STRING) AS id," +
      "CAST(input.tsArray AS STRING)  AS tsArray " +
      "FROM input"

    val lineStringDF = spark.sql(sqlQuery)

    //    lineStringDF.show(5)
    //    lineStringDF.printSchema()
    lineStringDF.repartition(numPartitions)

  }
}