package LastTerm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.DoubleType

object lastTerm_answer {

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

    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_final"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
    //      "salesid, salesname, productgroup, product, item," +
    //      "yearweek, year, week, " +
    //      "cast(qty as double) as qty," +
    //      "cast(target as double) as target," +
    //      "idx from selloutTable where 1=1"
    var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    // Global Variables //
    var VALID_YEAR = 2015
    var VALID_WEEK = 52
    var VALID_PRODUCT = Array("PRODUCT1","PRODUCT2").toSet
    var MAX_QTY_VALUE = 9999999.0
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x=>{

      // Data comes in line by line
      var checkValid = true
      // Assign yearweek information to variables
      var year = x.getString(yearweekNo).substring(0,4).toInt
      var week = x.getString(yearweekNo).substring(4,6).toInt
      // Assign abnormal to variables
      // filtering
      if ((week > VALID_WEEK) ||
        (year < VALID_YEAR) ||
        (!VALID_PRODUCT.contains(x.getString(productNo))))
      {
        checkValid = false
      }
      checkValid
    })

    // key, account, product, yearweek, qty, productname
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Transform         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      if(qty > MAX_QTY_VALUE){qty = MAX_QTY_VALUE}
      Row( x.getString(keyNo),
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Final #1         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd1 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      map(x=>{
        // GROUP BY 키값과 데이터를 정의하세요
        var key = x._1
        var data = x._2

        // 평균을 구하세요
        var sum_qty = data.map(x=>{x.getDouble(qtyNo)}).sum
        var size = data.size
        var avg = Math.round(sum_qty/size)

        // (KEY, VALUE)로
        (key,avg)
      })

    // 심심하면 만들어 보세요
    // groupRdd1.collectAsMap

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Final #2         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd2 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      flatMap(x=>{
        // GROUP BY 키값과 데이터를 정의하세요
        var key = x._1
        var data = x._2

        // 평균을 구하세요
        var sum_qty = data.map(x=>{x.getDouble(qtyNo)}).sum
        var size = data.size
        var avg = Math.round(sum_qty/size)

        // 각 데이터별 RATIO 를 구하세요 ratio = each_qty / avg
        var finalData = data.map(x=>{

          var ratio = 1.0d
          var each_qty = x.getDouble(qtyNo)
          ratio = each_qty/avg
          ratio = Math.round(ratio*100.0)/100.0d

          (x.getString(accountidNo),
            x.getString(productNo),
            x.getString(yearweekNo),
            x.getDouble(qtyNo),
            avg.toDouble,
            ratio.toDouble)})
        finalData
      })

    // Row를 정의하지 않으면 바로 데이터프레임 변환이 가능함
    // 마지막에  Row 붙인경우 StructType으로 사용
    var middleResult = groupRdd2.toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")
    println(middleResult.show)

    middleResult.createOrReplaceTempView("MIDDLETABLE")

    // 주차로 그룹바이 한 후 주차별 평균 RATIO를 산출하세요
    var  finalResult = spark.sql("" +
      " SELECT REGIONID, PRODUCT, SUBSTRING(YEARWEEK,5,6) AS WEEK, AVG(RATIO) AS AVG_RATIO " +
      " FROM MIDDLETABLE  " +
      " GROUP BY REGIONID, PRODUCT, SUBSTRING(YEARWEEK,5,6) ")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 9. Data unloading (memory -> oracle)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var outputUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var outputUser = "root"
    var outputPw = "P@ssw0rd"

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "kopo_test_result"
    finalResult.write.mode("overwrite").jdbc(outputUrl, table, prop)

    // 다풀고 심심하면 MySql에 데이터를 저장해보세요
  }
}


