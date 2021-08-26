import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Polygon}
import flowGeoSpark.{genGrids, genSTGrids}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{PolygonRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.Config

object flowSQL {
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

    val geometryFactory = new GeometryFactory()

    val pointDf = readPoints(dataFile)
    pointDf.createOrReplaceTempView("points")

    val polygons = stGrids.map(query => {
      val coordinates = new Array[Coordinate](5)
      coordinates(0) = new Coordinate(query._1(0), query._1(1))
      coordinates(1) = new Coordinate(query._1(0), query._1(3))
      coordinates(2) = new Coordinate(query._1(2), query._1(3))
      coordinates(3) = new Coordinate(query._1(2), query._1(1))
      coordinates(4) = coordinates(0)
      geometryFactory.createPolygon(coordinates)
    }
    )
    val queries = (polygons zip stGrids.map(_._2)).map(x => {
      val polygon = x._1
      polygon.setUserData(x._2)
      polygon
    })
    val queryRDD = new PolygonRDD(sc.parallelize(queries))
    val queryDf = Adapter.toDf(queryRDD, spark)
    queryDf.createOrReplaceTempView("queries")
    queryDf.printSchema()
    queryDf.show
    val sqlQuery = "SELECT * FROM points, queries " +
      "WHERE ST_Contains(points.location, queries.geometry)"

    val res = spark.sql(sqlQuery)

    res.show()
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
