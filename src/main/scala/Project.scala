import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType

object Project {
  case class PointData(trajectoryIdentification: String, objectIdentification: String, x: Double, y: Double, time: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[*]")
      .appName("SparkSessionForSimba")
      .config("simba.index.partitions", "20")
      .getOrCreate()

    part1(simbaSession)
//    question1(simbaSession)
//    question2(simbaSession)
    question3(simbaSession)
    simbaSession.stop()
    simbaSession.close()
  }

  private def part1(simba: SimbaSession): Unit = {
    var ds = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv").cache
    ds = ds.withColumnRenamed("_c0", "trajectoryIdentification")
    ds = ds.withColumnRenamed("_c1", "objectIdentification")
    ds = ds.withColumnRenamed("_c2", "x")
    ds = ds.withColumnRenamed("_c3", "y")
    ds = ds.withColumnRenamed("_c4", "timeRead")

    var ds2 = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/POIs.csv").cache

    ds2 = ds2.withColumnRenamed("_c0", "objectIdentification")
    ds2 = ds2.withColumnRenamed("_c1", "description")
    ds2 = ds2.withColumnRenamed("_c2", "x")
    ds2 = ds2.withColumnRenamed("_c3", "y")

    println("---------------------------------------------------------")
    ds.printSchema()
    ds2.printSchema()

    ds.createOrReplaceTempView("trajectory")

    ds2.createOrReplaceTempView("poi")

//    simba.indexTable("trajectory", RTreeType, "testtree",  Array("x", "y"))
//    simba.indexTable("poi", RTreeType, "poisIndex",  Array("x", "y") )
//    simba.loadIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")
//    simba.persistIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")
  }

  private def question1 (simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
        simba.sql("Select * from trajectory").range(Array("x", "y"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0))
          .where("description LIKE \"amenity=restaurant\"").show()
  }

  private def question2 (simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
    var tmp = simba.sql("Select objectIdentification, x, y, date_format(timeRead, 'u') as dow, date_format(timeRead, 'y') as Year," +
      " date_format(timeRead, 'w') as week, date_format(timeRead, 'H') as hour from trajectory where date_format(timeRead, 'u') <= 5")
      .circleRange(Array("x", "y"), Array(-322357.0, 4463408.0), 2000).select("objectIdentification", "dow",
      "Year", "week", "hour").distinct().cache()
    tmp.show(300)
    var count = tmp.count()
    var days = tmp.select("dow", "Year", "week", "hour").distinct().count()
    println("----------------------------- Visitors A Day -------------------------------")
    println("people found in the area: " + count)
    println("days tracked: " + days)
    println("people/hours: " + count.asInstanceOf[Double]/(days * 24))
  }

  private def question3 (simba:SimbaSession): Unit = {
    // -324297.5, 4461397.5
    // upper left  (-339220, 4461397.5)      (-324297.5, 4478070)
    // upper right (-324297.5, 4461397.5)    (-309375, 4478070)
    // lower left  (-339220, 4444725)    (-324297.5, 4461397.5)
    // lower right (-324297.5, 4444725)     (-309375, 4478070)
    import simba.simbaImplicits._
    var tmp = simba.sql("Select tt.* from (Select min(timeRead) as min, max(timeRead) as max, trajectoryIdentification from trajectory as t Group By trajectoryIdentification) as t" +
      " Inner Join trajectory as tt on t.trajectoryIdentification = tt.trajectoryIdentification and (tt.timeRead = t.max or tt.timeRead = t.min)")
      .range(Array("x", "y"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0))
    tmp.show()
    var totalPoints = tmp.groupBy("trajectoryIdentification").count().where("count = 2").count()
    var selected = tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-324297.5, 4461397.5) ,Array(-309375.0, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220.0, 4444725) ,Array(-324297.5, 4461397.5)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    println("total: " + totalPoints)
    println("end in same: " + (totalPoints - (selected - totalPoints)))
    println("end in different: " + (selected - totalPoints))
  }

}i  