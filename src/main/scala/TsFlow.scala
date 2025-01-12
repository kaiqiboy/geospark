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

object TsFlow {
  case class E(shape: String, timeStamp: Array[Long], v: Option[String], d: String)

  def main(args: Array[String]): Unit = {
    val dataFile = args(0)
    val numPartitions = args(1).toInt
    val queryFile = args(2)
    val tStep = args(3).toInt
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("TsFlow")
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
      val ts = splitTemporal(Array(q(4).toLong, q(5).toLong), tStep)
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
      }.rdd.map {
        case (geoms, timestamp, _) =>
          ts.map(r => if (timestamp <= r._2 && timestamp >= r._1) 1 else 0)
      }
      val r = combinedRDD.mapPartitions { p =>
        var res = ts.map(_ => 0)
        while (p.hasNext) {
          res = res.zip(p.next).map(x => x._1 + x._2)
        }
        Iterator(res)
      }.collect()
      val res = r.drop(1).foldLeft(r.head)((a, b) => a.zip(b).map { case (x, y) => x + y })
      println(res.take(5))
      combinedRDD.unpersist()
      pointRDD.rawSpatialRDD.unpersist()
      spark.catalog.clearCache()
    }
    println(s"ts flow ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def readEvent(file: String, numPartitions: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val readDs = spark.read.parquet(file)
    import spark.implicits._
    val pointRDD = readDs.as[E].rdd.map(e => {
      val content = ("""\([^]]+\)""".r findAllIn e.shape).next.drop(1).dropRight(1).split(" ")
      val coord = s"${content(0)},${content(1)}"
      (coord, e.d, e.timeStamp(0))
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

  def splitTemporal(temporalRange: Array[Long], tStep: Int): Array[(Long, Long)] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tSplit = ((tMax - tMin) / tStep).toInt
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield (t(0), t(1))
  }
}