import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType

object Project {
  case class PointData(trajectoryIdentification: String, objectIdentification: String, longitude: Double, latitude: Double, time: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[*]")
      .appName("SparkSessionForSimba")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    part1(simbaSession)
    simbaSession.stop()
    simbaSession.close()
  }

  private def part1(simba: SimbaSession): Unit = {
    val names: Array[String] = Array("trajectoryIdentification", "objectIdentification", "longitude", "latitude", "time")
    var df = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv")
    println("---------------------------------------------------------")
    df.printSchema()

//    df.createOrReplaceTempView("a")

//    simba.indexTable("a", RTreeType, "testtree",  Array("_c2", "_c3") )

//    simba.showIndex("a")

//    import simba.implicits._
//    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()
//
//    import simba.simbaImplicits._
//    caseClassDS.knn(Array("x", "y"),Array(1.0, 1.0),4).show(4)

  }
}
