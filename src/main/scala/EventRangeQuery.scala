import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source


case class E2(id: String, lon: Double, lat: Double, t: Long)


object EventRangeQuery {
  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (r.take(4).map(_.toDouble), r.takeRight(2).map(_.toLong))
    })

    var t = nanoTime
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkRangeQueryTraj")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val pointDf = readEvent(dataFile, numPartitions)
    pointDf.show(5)
    val pointRDD = Adapter.toSpatialRdd(pointDf, "location")
    pointRDD.analyze()
    pointRDD.buildIndex(IndexType.RTREE, false)
    pointRDD.indexedRawRDD.rdd.cache()
    println(pointRDD.rawSpatialRDD.count())
    println(s"Data loading ${(nanoTime - t) * 1e-9} s")
    t = nanoTime
    for (query <- queries) {
      val sQuery = new Envelope(query._1(0), query._1(2), query._1(1), query._1(3))
      val resultS = RangeQuery.SpatialRangeQuery(pointRDD, sQuery, true, true)
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .map {
          case (geoms, tsString) =>
            val timestamp = tsString.split("\t").head.toLong
            val id = tsString.split("\t")(1)
            (geoms, timestamp, id)
        }.rdd
        .filter {
          case (_, timestamp, _) => query._2(0) <= timestamp && query._2(1) >= timestamp
        }
      println(combinedRDD.count)
    }
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
}