import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.GeoJsonReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD, PolygonRDD}
import ReadQueries.readQueries

import java.lang.System.nanoTime

object GeoSparkRangeQuery {
  def main(args: Array[String]): Unit = {
    // spark
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("GeoSparkRangeQuery").setMaster("local[*]")
    //    val conf = new SparkConf().setAppName("GeoSparkRangeQuery")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // input data and parameters
    val dataFile = args(0)
    val allowTopologyInvalidGeometries = true // Optional for GeoJsonReader.readToGeometryRDD
    val skipSyntaxInvalidGeometries = false // Optional for GeoJsonReader.readToGeometryRDD
    val indexType = IndexType.RTREE


    val queries = readQueries(args(1))

    testSpatialRangeQueryUsingIndex()
    sc.stop()

    def testSpatialRangeQueryUsingIndex() {
      println("In function testSpatialRangeQueryUsingIndex: ")
      var t = nanoTime()
      val taxiRDD = GeoJsonReader.readToGeometryRDD(sc, dataFile, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
      // taxiRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      println(taxiRDD.rawSpatialRDD.count)
      val numPartitions = args(2).toInt
      taxiRDD.analyze()
      taxiRDD.spatialPartitioning(GridType.QUADTREE, numPartitions)
      println(s"... Build LineStringRDD: ${(nanoTime() - t) * 1e-9} s.")

      t = nanoTime()
      taxiRDD.buildIndex(indexType, true)
      taxiRDD.spatialPartitionedRDD.rdd.persist(StorageLevel.MEMORY_ONLY)
      println(s"... Build RTree index: ${(nanoTime() - t) * 1e-9} s.")

      t = nanoTime()
      for (query <- queries) {
        val tQuery = (query(4).toLong, query(5).toLong)
        val sQuery = new Envelope(query(0), query(1), query(2), query(3))
        val resultS = RangeQuery.SpatialRangeQuery(taxiRDD, sQuery, true, true)
        val resultST = resultS.map[String](f => f.getUserData.asInstanceOf[String]).filter(x => {
          val ts = x.split("\t")(7).toLong
          ts <= tQuery._2 && ts >= tQuery._1
        })
        val c = resultST.count
        println(c)
      }
      println(s"... Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    }
  }
}
