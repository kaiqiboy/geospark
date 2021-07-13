import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.geosparksql.expressions.ST_PointFromText
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

object flowGeoSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(Config.get("master")) // Delete this if run in cluster mode
      .appName("GeoSparkRangeQuery") // Change this to a proper name
      // Enable GeoSpark custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * "C:\\Users\\kaiqi001\\Documents\\GitHub\\geomesa-fs_2.12-3.2.0\\face-point\\09_W964092771efa4a4b87c1c75d5c79d6ec.parquet" "-8.65, 41.13, -8.57, 41.17" "1372636800,1372736800" 2 36000
     */
    val dataFile = args(0)
    val sQuery = args(1).split(",").map(_.toDouble)
    val tQuery = args(2).split(",").map(_.toLong)
    val sSize = args(3).toInt
    val tSplit = args(4).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)

    println(s"${stGrids.length} st grids.")

    val pointDf = readPoints(dataFile)

    //    pointDf.show(5)
    //    pointDf.printSchema()


    /** sql version */
    //    pointDf.createOrReplaceTempView("points")

    //    var res = new Array[(Array[Double], Array[Long], Int)](0)
    //
    //    for (query <- stGrids) {
    //      val tQuery = (query._2(0), query._2(1))
    //      val rangeQuery =
    //        s"SELECT * FROM points " +
    //          s"WHERE ST_Contains (ST_PolygonFromEnvelope(${query._1(0)},${query._1(1)},${query._1(2)},${query._1(3)}), location) " +
    //          s"AND timestamp >= ${tQuery._1} " +
    //          s"AND timestamp <= ${tQuery._2}"
    //
    //      val resDf = spark.sql(rangeQuery)
    //      //      resDf.show(5)
    //
    //      res = res :+ (query._1, query._2, resDf.count.toInt)
    //    }

    val pointRDD = Adapter.toSpatialRdd(pointDf, "location", List("timestamp"))
    pointRDD.analyze()
    pointRDD.spatialPartitioning(GridType.QUADTREE, sSize* sSize)
    pointRDD.buildIndex(IndexType.RTREE, false)
    var res = new Array[(Array[Double], Array[Long], Int)](0)

    for (query <- stGrids) {
      val tQuery = (query._2(0), query._2(1))
      val sQuery = new Envelope(query._1(0), query._1(2), query._1(1), query._1(3))
      val resultS = RangeQuery.SpatialRangeQuery(pointRDD, sQuery, true, true)
      val resultST = resultS.map[String](f => f.getUserData.asInstanceOf[String]).filter(x => {
        val ts = x.toLong
        ts <= tQuery._2 && ts >= tQuery._1
      })
      val c = resultST.count
      res = res :+ (query._1, query._2, c.toInt)
    }

    //    /** rdd version */
    //    val pointFile = "datasets/porto_points/part-00000"
    //
    //    val pointRDDInputLocation = pointFile
    //    val pointRDDOffset = 0 // The point long/lat starts from Column 0
    //    val pointRDDSplitter = FileDataSplitter.CSV
    //    val carryOtherAttributes = true // Carry Column 2 (hotel, gas, bar...)
    //    val pointRDD = new PointRDD(sc, pointRDDInputLocation,
    //      pointRDDOffset, pointRDDSplitter, carryOtherAttributes)
    //    pointRDD.analyze()
    //    pointRDD.spatialPartitioning(GridType.QUADTREE, 16)
    //    pointRDD.buildIndex(IndexType.RTREE, false)
    //
    //    var res = new Array[(Array[Double], Array[Long], Int)](0)
    //
    //    for (query <- stGrids) {
    //      val tQuery = (query._2(0), query._2(1))
    //      val sQuery = new Envelope(query._1(0), query._1(2), query._1(1), query._1(3))
    //      val resultS = RangeQuery.SpatialRangeQuery(pointRDD, sQuery, true, true)
    //      val resultST = resultS.map[String](f => f.getUserData.asInstanceOf[String]).filter(x => {
    //        val ts = x.toLong
    //        ts <= tQuery._2 && ts >= tQuery._1
    //      })
    //
    //      //      println(resultS.take(5))
    //
    //      val c = resultST.count
    //      res = res :+ (query._1, query._2, c.toInt)
    //    }
    res.foreach(x => println(x._1.deep, x._2.mkString("Array(", ", ", ")"), x._3))
    println(s"Total Points: ${res.map(_._3).sum}")

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

  def readPoints(file: String = "C:\\Users\\kaiqi001\\Documents\\GitHub\\geomesa-fs_2.12-3.2.0\\face-point\\09_W964092771efa4a4b87c1c75d5c79d6ec.parquet"):
  DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.parquet(file)
    //    df.show(5)
    df.createOrReplaceTempView("input")
    val sqlQuery = "SELECT ST_PointFromText(" +
      "TRIM( '[]' FROM CAST(input.geom AS STRING)), " +
      "',') " +
      "AS location, CAST(input.timestamp AS LONG) AS timestamp" +
      " FROM input"
    //      "AS location FROM input"

    spark.sql(sqlQuery)

  }
}
