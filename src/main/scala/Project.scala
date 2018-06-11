import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}

import scala.reflect.ClassTag
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer


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
//    question3(simbaSession)
//    question4(simbaSession)
//    question5(simbaSession)
    simbaSession.stop()
    simbaSession.close()
  }

  case class Data(trajectoryIdentification: Int, objectIdentification: Int, x: Double, y: Double, timeRead: Timestamp)
  case class mbr(minX: Double, maxX: Double, minY: Double, maxY: Double)
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)


  case class mbrs (mbrList: ListBuffer[mbr]) {
    def addMbr(input: Iterator[Row]): mbrs = {
      var init = false
      var maxX = 0.0
      var minX = 0.0
      var maxY = 0.0
      var minY = 0.0

      input.foreach { elem =>
        if(!init) {
          maxX = elem.get(2).asInstanceOf[Double]
          minX = elem.get(2).asInstanceOf[Double]
          maxY = elem.get(3).asInstanceOf[Double]
          minY = elem.get(3).asInstanceOf[Double]
          init = true
        }
        var x = elem.get(2).asInstanceOf[Double]
        var y = elem.get(3).asInstanceOf[Double]

        if (x > maxX)
          maxX = x
        if(x <=minX)
          minX = x
        if(y > maxY)
          maxY = y
        if(y < minY)
          minY = y

      }
//      println( mbr(minX, maxX, minY, maxY).toString )
      mbrList += mbr(minX, maxX, minY, maxY)
      this
    }

    def merge(other: mbrs): mbrs ={
      other.mbrList.foreach(mbr => {
        mbrList += mbr
      })
      this
    }

  }


  private def part1(simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
    var ds = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/trajectories.csv")
    ds = ds.withColumnRenamed("_c0", "trajectoryIdentification")
    ds = ds.withColumnRenamed("_c1", "objectIdentification")
    ds = ds.withColumnRenamed("_c2", "x")
    ds = ds.withColumnRenamed("_c3", "y")
    ds = ds.withColumnRenamed("_c4", "timeRead")

    var ds2 = simba.read.option("inferSchema", "true").csv("/home/pgiorgianni/Downloads/POIs.csv")

    ds2 = ds2.withColumnRenamed("_c0", "objectIdentification")
    ds2 = ds2.withColumnRenamed("_c1", "description")
    ds2 = ds2.withColumnRenamed("_c2", "x")
    ds2 = ds2.withColumnRenamed("_c3", "y")

//    ds.printSchema()
//    ds2.printSchema()

    ds.createOrReplaceTempView("trajectory")

    ds2.createOrReplaceTempView("poi")

    simba.indexTable("trajectory", RTreeType, "trajectoriesIndex",  Array("x", "y"))
    val result = ds.toDF().mapPartitions(partition =>
      Iterator(mbrs(ListBuffer()).addMbr(partition))).reduce((x,y) => x.merge(y))
//    println(result.mbrList.size)
//    simba.showIndex("trajectory")
//    simba.sql("SELECT INDEX trajectoriesIndex")
//    simba.indexTable("poi", RTreeType, "poisIndex",  Array("x", "y") )

    //    simba.loadIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")
//    simba.persistIndex("poisIndex", "/home/pgiorgianni/Downloads/POIsIndex")
  }

  private def question1 (simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
    //just a simple range suery on the trajectory with a like filter on description to find the restaurants
    simba.sql("Select * from poi").range(Array("x", "y"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0))
      .where("description LIKE \"%restaurant%\"").show()
  }

  private def question2 (simba: SimbaSession): Unit = {
    import simba.simbaImplicits._
    //use sql to select the point and break the date into fields of day of week, week of year, hour of day, and year. then do a range query on them.
    var tmp = simba.sql("Select objectIdentification, x, y, date_format(timeRead, 'u') as dow, date_format(timeRead, 'y') as Year," +
      " date_format(timeRead, 'w') as week, date_format(timeRead, 'H') as hour from trajectory where date_format(timeRead, 'u') <= 5")
      .circleRange(Array("x", "y"), Array(-322357.0, 4463408.0), 2000).select("objectIdentification", "dow",
      "Year", "week", "hour").distinct().cache()
    tmp.show(20)
//    simba.sql("SELECT max(timeRead), min(timeRead) from trajectory where where date_format(timeRead, 'u') <= 5").show()
    var count = tmp.count()
    //sort by unique hour and object to see how many objects are in the data
    var days = tmp.select("dow", "Year", "week").distinct().count()
    //output how many people were in the square divided by number of days * 24 hours that people were in the area
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
    //selects all points by the max and min times and then does a range query to get the area we are working it
    var tmp = simba.sql("Select tt.* from (Select min(timeRead) as min, max(timeRead) as max, trajectoryIdentification from trajectory as t Group By trajectoryIdentification) as t" +
      " Inner Join trajectory as tt on t.trajectoryIdentification = tt.trajectoryIdentification and (tt.timeRead = t.max or tt.timeRead = t.min)")
      .range(Array("x", "y"),Array(-339220.0,  4444725),Array(-309375.0, 4478070.0))
    tmp.show()
    //he would prefer us to take the centroids of the involved points.
    //counts the number of points that have start and end in the range
    var totalPoints = tmp.groupBy("trajectoryIdentification").count().where("count = 2").count()
    //counts the number of points in each rectangle
    var selected = tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-324297.5, 4461397.5) ,Array(-309375.0, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220.0, 4444725) ,Array(-324297.5, 4461397.5)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    selected += tmp.range(Array("x", "y"),Array(-339220, 4461397.5) ,Array(-324297.5, 4478070)).groupBy("trajectoryIdentification").count().where("count = 2").count()
    //outputs the total number of points that start and end in the same range, start and end in a different range, and total
    println("total: " + totalPoints)
    println("end in same: " + (totalPoints - (selected - totalPoints)))
    println("end in different: " + (selected - totalPoints))
  }

  def question4(ds : SimbaSession) : Unit ={
    import ds.simbaImplicits._
    var df = ds.sql("Select x,y from trajectory where date_format(timeRead,'m') <=  6 and date_format(timeRead,'m') >= 2")
    var df2 = df.distinct()
    df2.distanceJoin(df,Array("x","y"),Array("x","y"),20.0).groupBy("x","y").count().sort(desc("count")).limit(20).show()
  }

  def question5(simba : SimbaSession) : Unit ={
    import simba.simbaImplicits._
    //gets the values of each dataset and does some filtering for weekdays
    var poi = simba.sql("Select x as poix, y as poiy, objectIdentification as id from poi")
    var trajectories = simba.sql("Select x, y, objectIdentification, date_format(timeRead, 'M') as Month," +
      " date_format(timeRead, 'y') as year from trajectory where where date_format(timeRead, 'u') <= 5 and " +
      "(date_format(timeRead, 'y') = 2008 or date_format(timeRead, 'y') = 2009)")
    //joins the sets where they are x distance appart then groups them by point and sorts them to count
    var solution = poi.distanceJoin(trajectories, Array("poix", "poiy"), Array("x", "y"), 100).select("id",
      "Month", "year").distinct().cache()//.groupBy("poix", "poiy").count().sort(desc("count")).limit(20)
    var twoThousandAnd8 = solution.where("year = 2008").groupBy("id", "month").count().sort(desc("count")).select("id").cache()
    println("January 2008")
    twoThousandAnd8.where("month = 1").show(10)
    println("February 2008")
    twoThousandAnd8.where("month = 2").show(10)
    println("March 2008")
    twoThousandAnd8.where("month = 3").show(10)
    println("April 2008")
    twoThousandAnd8.where("month = 4").show(10)
    print("May 2008")
    twoThousandAnd8.where("month = 5").show(10)
    println("June 2008")
    twoThousandAnd8.where("month = 6").show(10)
    println("July 2008")
    twoThousandAnd8.where("month = 7").show(10)
    println("August 2008")
    twoThousandAnd8.where("month = 8").show(10)
    println("September 2008")
    twoThousandAnd8.where("month = 9").show(10)
    println("October 2008")
    twoThousandAnd8.where("month = 10").show(10)
    println("November 2008")
    twoThousandAnd8.where("month = 11").show(10)
    println("December 2008")
    twoThousandAnd8.where("month = 12").show(10)

    var twoThousandAnd9 = solution.where("year = 2009").groupBy("id", "month").count().sort(desc("count")).select("id").cache()
    println("January 2009")
    twoThousandAnd9.where("month = 1").show(10)
    println("February 2009")
    twoThousandAnd9.where("month = 2").show(10)
    println("March 2009")
    twoThousandAnd9.where("month = 3").show(10)
    println("April 2009")
    twoThousandAnd9.where("month = 4").show(10)
    print("May 2009")
    twoThousandAnd9.where("month = 5").show(10)
    println("June 2009")
    twoThousandAnd9.where("month = 6").show(10)
    println("July 2009")
    twoThousandAnd9.where("month = 7").show(10)
    println("August 2009")
    twoThousandAnd9.where("month = 8").show(10)
    println("September 2009")
    twoThousandAnd9.where("month = 9").show(10)
    println("October 2009")
    twoThousandAnd9.where("month = 10").show(10)
    println("November 2009")
    twoThousandAnd9.where("month = 11").show(10)
    println("December 2009")
    twoThousandAnd9.where("month = 12").show(10)

    //    solution.show(20)
  }

}
