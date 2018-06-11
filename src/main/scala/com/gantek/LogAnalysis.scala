package com.gantek

import org.apache.spark.sql._
import org.apache.log4j._

import scala.collection.immutable.ListMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col

object LogAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    Logger.getLogger("com.hortonworks.spark.Logs").setLevel(Level.INFO)

    val log = Logger.getLogger("com.hortonworks.spark.Logs")

    log.info("Started Logs Analysis")
      val spark = SparkSession.builder()
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate

        val p = new AccessLogParser
        val logFile = spark.sparkContext.textFile("/home/wild/projects/Log_Analyzer/data/access.log")
        val accessLogs = logFile.map(line=>p.parseRecordReturningNullObjectOnFailure(line))


        import spark.implicits._

        val df1 = accessLogs.toDF()
        df1.createOrReplaceTempView("accessLogsDF")
        df1.printSchema()
        df1.describe("bytesSent").show()
        df1.first()
        df1.head()
        df1.explain()
        val df2=df1.groupBy("userAgent").count()


      // Calculate statistics based on the content size.
      val contentSizes = accessLogs.map(log => log.bytesSent)
      val contentTotal = contentSizes.reduce(_ + _)

    val uriCount = logFile.map(p.parseRecordReturningNullObjectOnFailure(_).request)
      .filter(request => request != "")  // filter out records that wouldn't parse properly
      .map(_.split(" ")(1))              // get the uri field
      .map(uri => (uri, 1))              // create a tuple for each record
      .reduceByKey((a, b) => a + b)      // reduce to get this for each record: (/java/java_oo/up.png,2)
      .collect                           // convert to Array[(String, Int)], which is Array[(URI, numOccurrences)]

    val uriHitCount = ListMap(uriCount.toSeq.sortWith(_._2 > _._2):_*)
    uriHitCount.take(20).foreach(println)

    println("Uniqe count of requests according to Months:")
    groupDataByMonth(df1).collect().foreach(println)
    spark.stop()

  }

  def groupDataByMonth(df:DataFrame):DataFrame={
    import java.util.Calendar
    val extractDay = udf((x:String) => {
      val cal = Calendar.getInstance
      val date= AccessLogParser.parseDateField(x)
      for{
        d <- date

      }yield {
        cal.setTimeInMillis(d.getTime)
        cal.get(Calendar.MONTH)

      }
    })

    val df4 = df.withColumn("Day", extractDay(col("dateTime")))
    df4.select("clientIpAddress","Day").distinct.groupBy("Day").count

  }

}
