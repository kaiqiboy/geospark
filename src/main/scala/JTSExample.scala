import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.Config

object JTSExample extends App {
  val gf = new GeometryFactory()
  val queryWindow = new Envelope(-8.682329739182336, -8.553892156181982,
    41.16930767535641, 41.17336956864337)
  val points = Array((-8.600112, 41.182704), (-8.599743, 41.182704), (-8.59797, 41.182236), (-8.597853, 41.182191), (-8.597853, 41.182182), (-8.597844, 41.182173), (-8.597835, 41.182173), (-8.597826, 41.182164), (-8.596692, 41.182056), (-8.59437, 41.181354), (-8.591373, 41.180121), (-8.588907, 41.179608), (-8.586936, 41.179617), (-8.585649, 41.180364), (-8.583822, 41.181273), (-8.583651, 41.181201), (-8.58366, 41.181183), (-8.58366, 41.181192), (-8.583588, 41.181156), (-8.582373, 41.180526), (-8.581644, 41.180094), (-8.581014, 41.179257), (-8.580105, 41.178789), (-8.580357, 41.178474))
  for (p <- points) {
    val coord = new Coordinate(p._1, p._2)
    print(p)
    val point = gf.createPoint(coord)
    point.getCoordinates()(0).x
    println(queryWindow.contains(coord))
  }
}

object GeoSparkSqlTest extends App {
  val spark = SparkSession.builder()
    .master(Config.get("master"))
    .appName("RasterFlow")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(spark)
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val df = spark.sql("SELECT ST_PointFromText('40.7128,-74.0060', ',') AS pointshape")
  df.printSchema()
  df.show()

  val data = Seq(Row(100, "Fremont", "Honda Civic", 10),
    Row(100, "Fremont", "Honda Accord", 15),
    Row(100, "Fremont", "Honda CRV", 7),
    Row(200, "Dublin", "Honda Civic", 20),
    Row(200, "Dublin", "Honda Accord", 10),
    Row(200, "Dublin", "Honda CRV", 3),
    Row(300, "San Jose", "Honda Civic", 5),
    Row(300, "San Jose", "Honda Accord", 8))
  val schema = new StructType()
    .add("id", IntegerType, true)
    .add("city", StringType, true)
    .add("car_model", StringType, true)
    .add("quantity", IntegerType, true)
  val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
  val df2 = spark.createDataFrame(sc.parallelize(data), schema)
  df2.createOrReplaceTempView("dealer")
  spark.sql("SELECT id, collect_list(quantity) FROM dealer GROUP BY id ORDER BY id").printSchema()
}

object test2 extends App {
  val spark = SparkSession.builder()
    .master(Config.get("master"))
    .appName("RasterFlow")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()
  GeoSparkSQLRegistrator.registerAll(spark)
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val f = "C:\\Users\\kaiqi001\\Documents\\GitHub\\geospark\\src\\test\\resources\\county_small.tsv"
  var rawDf = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(f)
  rawDf.createOrReplaceTempView("rawdf")
  //  rawDf.show()

  var spatialDf = spark.sql(
    """
      |SELECT ST_GeomFromWKT(_c0) AS countyshape, _c1, _c2
      |FROM rawdf
  """.stripMargin)
  spatialDf.createOrReplaceTempView("spatialdf")

  spatialDf.show()
  spatialDf = spark.sql(
    """
      |SELECT ST_Transform(countyshape, "epsg:4326", "epsg:3857") AS newcountyshape, _c1, _c2
      |FROM spatialdf
  """.stripMargin)
  spatialDf.createOrReplaceTempView("spatialdf")
  spatialDf.show()

  spatialDf = spark.sql(
    """
      |SELECT ST_Distance(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), newcountyshape) AS distance
      |FROM spatialdf
      |ORDER BY distance DESC
      |LIMIT 5
  """.stripMargin)
  spatialDf.show()
  Thread.sleep(10000000)
}

