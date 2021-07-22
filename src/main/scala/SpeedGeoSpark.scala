import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, LineString}
import geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

import scala.math.{abs, acos, cos, sin}

object SpeedGeoSpark {

  case class T(fid: String, timestamp: Long, points: (Array[Double], Array[Double]))

  case class Trajectory(points: Array[((Double, Double), Long)], id: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(Config.get("master"))
      .appName("GeoSparkSpeedQuery")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * "C:\Users\kaiqi001\Documents\GitHub\geomesa-fs_2.12-3.2.0\geomesa_traj_example\13_W0a4ad84eabe5444d827a9dbf1abbbd5b.parquet" "-8.65, 41.13, -8.57, 41.17" "1372636800,1372736800" 2 36000
     */
    val dataFile = args(0)
    val sQuery = args(1).split(",").map(_.toDouble)
    val tQuery = args(2).split(",").map(_.toLong)
    val sSize = args(3).toInt
    val tSplit = args(4).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)

    val trajDf = readTraj(dataFile)

    //    trajDf.printSchema()
    //    trajDf.show(5)
    val trajRDD = Adapter.toSpatialRdd(trajDf, "linestring")
    trajRDD.analyze()
    trajRDD.buildIndex(IndexType.RTREE, false)
    trajRDD.indexedRawRDD.rdd.cache()

    var res = new Array[(Array[Double], Array[Long], Double)](0)

    for (query <- stGrids) {
      val tQuery = (query._2(0), query._2(1))
      val sQuery = new Envelope(query._1(0), query._1(2), query._1(1), query._1(3))
      val resultS = RangeQuery.SpatialRangeQuery(trajRDD, sQuery,
        true, true)
      //      val resultS = trajRDD.rawSpatialRDD
      val combinedRDD = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
        .rdd.map {
        case (geoms, tsString) => {
          val points = geoms.getCoordinates.map(x => (x.x, x.y))
          val timestamps = tsString.split("\\[").last.dropRight(1).split(", ").map(_.toLong)
          val id = tsString.split(" ")(1)
          Trajectory(points zip timestamps, id)
        }
      }

      //      val countRDD = combinedRDD.map(x => {
      //        x.points.zipWithIndex.count {
      //          case ((s, t), _) => {
      //            s._1 >= sQuery.getMinX && s._2 >= sQuery.getMinY && s._1 <= sQuery.getMaxX && s._2 <= sQuery.getMaxY &&
      //              t >= tQuery._1 && t <= tQuery._2
      //          }
      //        }
      //      })
      //      println(countRDD.sum())

      val results = combinedRDD.map(x => {
        val pointsInside = x.points.zipWithIndex.filter {
          case ((s, t), _) => {
            s._1 >= sQuery.getMinX && s._2 >= sQuery.getMinY && s._1 <= sQuery.getMaxX && s._2 <= sQuery.getMaxY &&
              t >= tQuery._1 && t <= tQuery._2
          }
        }
        if (pointsInside.length < 2) None
        else {
          val distance = greatCircleDistance(pointsInside.last._1._1, pointsInside.head._1._1)
          val duration = pointsInside.last._1._2 - pointsInside.head._1._2
          Some(distance / duration)
        }
      })

      val validRDD = results.filter(_.isDefined).map(_.get)
      val avgSpeed = if (validRDD.count > 0) validRDD.filter(_ > 0).reduce(_ + _) / validRDD.count else 0
      res = res :+ (query._1, query._2, avgSpeed)
    }
    //      val resultST = resultS.map[(Geometry, String)](f => (f, f.getUserData.asInstanceOf[String]))
    //        .filter { case (_, x) => {
    //          val ts = x.split("\\[").last.dropRight(1).split(", ").map(_.toLong)
    //          val instants = ts.zipWithIndex.filter(t => t._1 <= tQuery._2 && t._1 >= tQuery._1)
    //          !instants.isEmpty
    //        }
    //        }.map { case (geom, x) => {
    //        val ts = x.split("\\[").last.dropRight(1).split(", ").map(_.toLong)
    //        val stPointsWithIndex = (geom.getCoordinates zip ts).zipWithIndex
    //        val valid = stPointsWithIndex.filter {
    //          case ((s, t), _) => sQuery.contains(s) && t <= tQuery._2 && t >= tQuery._1
    //        }
    //        if (valid.isEmpty) None
    //        else {
    //          val duration = valid.last._1._2 - valid.head._1._2-
    //          val distance = greatCircleDistance(valid.last._1._1, valid.head._1._1)
    //          Some(distance / duration)
    //        }
    //      }
    //      }
    //        .filter(x => x.asInstanceOf[Option[Double]].isDefined)
    //      val c = resultST.collect.toArray.map(x => x.asInstanceOf[Option[Double]].get).sum / resultST.count
    //      res = res :+ (query._1, query._2, c)

    res.foreach(x => println(x._1.deep, x._2.mkString("Array(", ", ", ")"), x._3))

    sc.stop()
  }


  def readTraj(file: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    def toString(points: String): String = {
      val coords = points.split("],")
      val xs = coords(0).replace("[", "").replace("]", "").split(",")
      val ys = coords(1).replace("[", "").replace("]", "").split(",")
      val r = xs zip ys
      var res = ""
      for (i <- r) res = res + i._1 + "," + i._2 + ","
      res.dropRight(1)
    }

    val toStringUDF = udf(toString _)

    def getTimeStamps(points: String, tStart: Long, tInterval: Int = 15): Array[Long] = {
      val coords = points.split("],")
      val length = coords(0).split(",").length
      (0 until length).toArray.map(x => tStart + x * tInterval)
    }

    val getTimeStampUDF = udf(getTimeStamps _)

    val df = spark.read.parquet(file)
      .filter("fid is not null")
      .filter("points is not null")
      .filter("timestamp is not null")
      .withColumn("string", toStringUDF($"points".cast("String")))
      .withColumn("tsArray", getTimeStampUDF($"points".cast("String"), $"timestamp", lit(15)))

    df.createOrReplaceTempView("input")

    val sqlQuery = "SELECT ST_LineStringFromText(CAST(input.string AS STRING), ',') AS linestring, " +
      "CAST(input.fid AS STRING) AS id," +
      "CAST(input.tsArray AS STRING)  AS tsArray " +
      "FROM input"

    val lineStringDF = spark.sql(sqlQuery)
    lineStringDF

  }

  def genGrids(range: Array[Double], size: Int): Array[Array[Double]] = {
    val lonMin = range(0)
    val latMin = range(1)
    val lonMax = range(2)
    val latMax = range(3)
    val lons = ((lonMin until lonMax by (lonMax - lonMin) / size) :+ lonMax).sliding(2).toArray
    val lats = ((latMin until latMax by (latMax - latMin) / size) :+ latMax).sliding(2).toArray
    lons.flatMap(x => lats.map(y => Array(x(0), y(0), x(1), y(1))))
  }

  def genSTGrids(grids: Array[Array[Double]], tRange: (Long, Long), tSplit: Int): Array[(Array[Double], Array[Long])] = {
    val tSlots = ((tRange._1 until tRange._2 by tSplit.toLong).toArray :+ tRange._2).sliding(2).toArray
    grids.flatMap(grid => tSlots.map(t => (grid, t)))
  }

  //  def greatCircleDistance(c1: Coordinate, c2: Coordinate): Double = {
  //    val x1 = c1.x
  //    val x2 = c2.x
  //    val y1 = c1.y
  //    val y2 = c2.y
  //    val r = 6371009 // earth radius in meter
  //    val phi1 = y1.toRadians
  //    val lambda1 = x1.toRadians
  //    val phi2 = y2.toRadians
  //    val lambda2 = x2.toRadians
  //    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
  //    r * deltaSigma
  //  }

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
}
