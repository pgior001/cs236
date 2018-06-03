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
    var ds = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv").cache()
    ds = ds.withColumnRenamed("_c0", "trajectoryIdentification")
    ds = ds.withColumnRenamed("_c1", "objectIdentification")
    ds = ds.withColumnRenamed("_c2", "longitude")
    ds = ds.withColumnRenamed("_c3", "latitude")
    ds = ds.withColumnRenamed("_c4", "timeRead")

    var ds2 = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/POIs.csv").cache()

    ds2 = ds2.withColumnRenamed("_c0", "objectIdentification")
    ds2 = ds2.withColumnRenamed("_c1", "description")
    ds2 = ds2.withColumnRenamed("_c2", "longitude")
    ds2 = ds2.withColumnRenamed("_c3", "latitude")

    println("---------------------------------------------------------")
    ds.printSchema()
    ds2.printSchema()
    ds2.show(20)

    ds.createOrReplaceTempView("trajectory")

    ds2.createOrReplaceTempView("poi")

//    simba.indexTable("trajectory", RTreeType, "testtree",  Array("longitude", "latitude") )
//    simba.indexTable("poi", RTreeType, "poisIndex",  Array("longitude", "latitude") )
//    simba.loadIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")
//    simba.persistIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")

    import simba.simbaImplicits._
    ds2.range(Array("longitude", "latitude"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0)).where("description LIKE \"amenity=restaurant\"").show()
    //http://epsg.io/4799 shows that the unit of this system is a meter. so is should be good like this
    ds.filter("date_format(timeRead, 'u') < 5").circleRange(Array("longitude", "latitude"), Array(-322357.0, 4463408.0), 2000).show()
  }
}
