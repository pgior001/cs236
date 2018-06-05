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
<<<<<<< HEAD
    var ds = simba.read.option("inferSchema", "true").csv("/home/kjkamgar/Downloads/trajectories.csv").cache()
=======
    var ds = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv").cache
>>>>>>> f55aa0a071fadf67d69e671312060346bd2c114b
    ds = ds.withColumnRenamed("_c0", "trajectoryIdentification")
    ds = ds.withColumnRenamed("_c1", "objectIdentification")
    ds = ds.withColumnRenamed("_c2", "x")
    ds = ds.withColumnRenamed("_c3", "y")
    ds = ds.withColumnRenamed("_c4", "timeRead")

<<<<<<< HEAD
    var ds2 = simba.read.option("inferSchema", "true").csv("/home/kjkamgar/Downloads/POIs.csv").cache()
=======
    var ds2 = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/POIs.csv").cache
>>>>>>> f55aa0a071fadf67d69e671312060346bd2c114b

    ds2 = ds2.withColumnRenamed("_c0", "objectIdentification")
    ds2 = ds2.withColumnRenamed("_c1", "description")
    ds2 = ds2.withColumnRenamed("_c2", "x")
    ds2 = ds2.withColumnRenamed("_c3", "y")

    println("---------------------------------------------------------")
    ds.printSchema()
    ds2.printSchema()

    ds.createOrReplaceTempView("trajectory")

    ds2.createOrReplaceTempView("poi")
    


<<<<<<< HEAD
//    simba.indexTable("trajectory", RTreeType, "testtree",  Array("longitude", "latitude") )
//    simba.indexTable("poi", RTreeType, "poisIndex",  Array("longitude", "latitude") )
//    simba.loadIndex("poisIndex", "~/Downloads/POIsIndex")
//    simba.persistIndex("poisIndex", "~/Downloads/POIsIndex")
=======
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
>>>>>>> f55aa0a071fadf67d69e671312060346bd2c114b

  private def question2 (simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
<<<<<<< HEAD
//    ds2.range(Array("longitude", "latitude"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0)).where("description LIKE \"amenity=restaurant\"").show()
//    ds2.sqlContext.sql("SELECT * FROM trajectory WHERE POINT(longitude, latitude) IN RANGE(POINT(-339220.0,  4444725), POINT(-309375.0, 4478070.0))").show()
    //http://epsg.io/4799 shows that the unit of this system is a meter. so is should be good like this
    var tmp = ds.filter("date_format(timeRead, 'u') <= 5").circleRange(Array("longitude", "latitude"), Array(-322357.0, 4463408.0), 2000)
    

  //  tmp.sortBy(    
    

//    tmp.show()
//    tmp.select("objectIdentification", "timeRead").groupBy("objectIdentification")
    println("------------------------------------------------------------------")
  }

  def question(ds : SimbaSession) : Unit ={
    // -324297.5  4461397.5
    // upper left  (-339220, 4478070)      (-324297.5, 4461397.5)
    // upper right (-324297.5, 4478070)    (-309375, 4461397.5)
    // lower left  (-339220, 4461397.5)    (-324297.5, 4444725)  
    // lower right (-324297.5,4478070)     (-309375, 4444725)
    
      
   
  }

  def question4(ds : SimbaSession) : Unit ={
     var df = ds.sql("Select * from trajectories where date_format(timeRead,'m') <=  6 and date_format(timeread,'m') >= 2")

     //ds2 = ds.select("*").from(ds).where("da <= 4 AND date >= 10")
     //ds.select(Array("x","y")).from(ds).circlerangle(
  }
}



=======
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
    //he would prefer us to take the centroids of the involved points.
    var totalPoints = tmp.groupBy("trajectoryIdentification").count().where("count = 2").count()
    var selected = tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-324297.5, 4461397.5) ,Array(-309375.0, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220.0, 4444725) ,Array(-324297.5, 4461397.5)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    println("total: " + totalPoints)
    println("end in same: " + (totalPoints - (selected - totalPoints)))
    println("end in different: " + (selected - totalPoints))
  }

}
>>>>>>> f55aa0a071fadf67d69e671312060346bd2c114b
