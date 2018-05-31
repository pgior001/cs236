import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType

object Project {
  case class PointData(trajectoryIdentification: String, objectIdentification: String, longitude: Double, latitude: Double, time: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[*]")
      .appName("SparkSessionForSimba")
      .config("simba.index.partitions", "20")
      .getOrCreate()

    part1(simbaSession)
    simbaSession.stop()
    simbaSession.close()
  }

  private def part1(simba: SimbaSession): Unit = {
    var ds = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv")
    ds = ds.withColumnRenamed("_c0", "trajectoryIdentification")
    ds = ds.withColumnRenamed("_c1", "objectIdentification")
    ds = ds.withColumnRenamed("_c2", "longitude")
    ds = ds.withColumnRenamed("_c3", "latitude")
    ds = ds.withColumnRenamed("_c4", "timeRead")

    println("---------------------------------------------------------")
    ds.printSchema()
//    ds.createOrReplaceTempView("trajectory")
//
//    simba.indexTable("trajectory", RTreeType, "testtree",  Array("longitude", "latitude") )
//
    import simba.simbaImplicits._
    ds.range(Array("longitude", "latitude"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0)).show()
    ds.circleRange(Array("longitude", "latitude"), Array(-322357.0, 4463408.0), 0.018018018).show()
//    simba.showIndex("trajectory")
  }
}
