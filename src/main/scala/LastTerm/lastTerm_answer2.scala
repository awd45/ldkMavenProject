package LastTerm

import org.apache.spark.{SparkConf, SparkContext}

object lastTerm_answer2 {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 1. Data Loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    staticUrl= "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_final"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("SELLOUT_VIEW")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. Data Processing
    //////////////////////////////////////////////////////////////////////////////////////////////////

    var finalResult = spark.sql("" +
      "SELECT C.REGIONID, C.PRODUCT, " +
      "SUBSTR(C.YEARWEEK,5,2) AS WEEK, " +
      "ROUND(AVG(C.RATIO),2) AS AVG_RATIO FROM ( " +
      "SELECT B.*, " +
      " ROUND(CASE WHEN B.AVG_QTY = 0 THEN 1 " +
      "       ELSE B.QTY/B.AVG_QTY END , 2) AS RATIO " +
      "FROM ( " +
      "       SELECT A.*," +
      "       ROUND(AVG(A.QTY) OVER(PARTITION BY A.REGIONID, A.PRODUCT),2) " +
      "       AS AVG_QTY" +
      "       FROM SELLOUT_VIEW A " +
      "       WHERE 1=1 " +
      "       AND SUBSTR(A.YEARWEEK,1,4) >= 2015 " +
      "       AND SUBSTR(A.YEARWEEK,5,2) != 53 " +
      "       AND A.PRODUCT IN ('PRODUCT1','PRODUCT2') ) B ) C " +
      "GROUP BY C.REGIONID, C.PRODUCT, SUBSTR(C.YEARWEEK,5,2) " )

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. Data unloading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var outputUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var outputUser = "kopo"
    var outputPw = "kopo"

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "KOPO_SPARK_ST_김효관3"

    finalResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
  }
}
