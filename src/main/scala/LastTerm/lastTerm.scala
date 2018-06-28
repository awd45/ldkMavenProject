package LastTerm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.{StringType, DoubleType,StructField, StructType}

object lastTerm{
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
    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
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
    // 2. data refining #1
    //////////////////////////////////////////////////////////////////////////////////////////////////
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
    // 다풀고 심심하면 파라미터를 오라클에 파라미터 테이블로 정의하고 불러와 보세요
    // groupRdd1.collectAsMap

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data refining #2
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information
    var filterRdd = rawRdd.filter(x=>{

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
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 4. data processing
    //////////////////////////////////////////////////////////////////////////////////////////////////
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
    // output: key, account, product, yearweek, qty, productname

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 5. Exam #1
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var groupRdd1 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      map(x=>{
        // GROUP BY 키값과 데이터를 변수에 정의합니다
        var key = x._1
        var data = x._2

        // avg 변수에 판매량의 평균을 구하세요
        // 답안을 제출하세요
        // Spark 기말고사1
        // var avg = data.
        var avg =1.0d
        var dataSize = data.size

        var sumation = data.map(x => {
          x.getDouble(qtyNo)
        }).sum

        var average = 0.0
        if (dataSize == 0) {
          average = 0.0d
        } else {
          average = sumation / dataSize

        }

        // (KEY, VALUE)로 출력합니다
        (key,avg)
      })
    // output: (key, avg)

    // 다풀고 심심하면 아래문구 실행해보세요 보세요
    // groupRdd1.collectAsMap

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 6. Exam #2
    //////////////////////////////////////////////////////////////////////////////////////////////////

    var groupRdd2 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),
        x.getString(productNo))}).
      flatMap(x=>{
        // GROUP BY 키값과 데이터를 변수에 정의합니다
        var key = x._1
        var data = x._2

        // avg 변수에 판매량의 평균을 구하세요 (반올림 예: 2222.0)
        // Spark 기말고사1
        // var avg = data.
        var avg = 1.0d
        var size=x._2.map(x=>{x.getDouble(qtyNo)}).size

        var sumation = data.map(x => {
          x.getDouble(qtyNo)
        }).sum


        if (size == 0) {
          avg = 0.0d
        } else {
          avg = Math.round(sumation / size)
        }


        // 각 데이터별 RATIO 를 구하세요 ratio = each_qty / avg

        var finalData = data.map(x => {

          // Ratio를 구하세요 Ratio = each_qty / qty
          // 디버깅시  map 또는 flatmap안에 map은 .first가
          // 아닌 .head로 불러와야 합니다.
          // Spark 기말고사2
          // 답안을 제출하세요
          // ratio =
          var ratio = 1.0d
          var each_qty = x.getDouble(qtyNo)

          ratio = each_qty / avg

          (x.getString(accountidNo),
            x.getString(productNo),
            x.getString(yearweekNo),
            x.getDouble(qtyNo),
            avg.toDouble,
            ratio.toDouble)
        })
        finalData
      })

    // output: (accountid, product,yearweek, qty, avg_qty, ratio)

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 7. Data converting (RDD -> Dataframe)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // Row를 정의하지 않으면 바로 데이터프레임 변환이 가능함
    // 마지막에  Row 붙인경우 StructType으로 사용



    var middleResult = groupRdd2.toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")
    println(middleResult.show)

    middleResult.createOrReplaceTempView("FINALTABLE")
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 8. Exam #3
    //////////////////////////////////////////////////////////////////////////////////////////////////





    // 지역, 상품, 주차 정보로 집계 한 후 지역, 상품, 주차별 평균 RATIO를 산출하세요
    // 만약 풀지 못할경우 middleResult를 결과 테이블로 오라클에 저장하세요.
    // 만약 풀어낼 경우 마지막에 oracle 연동 테이블 데이터프레임 이름을 middleResult -> finalResult로 변경후 오라클에 저장하세요.
    // 출력 컬럼은 -> REGIONID, PRODUCT, WEEK, AVG_RATIO  4개로 구성됩니다.

    // Spark 기말고사3
    // var  finalResult = spark.sql("select.....")
    // output: (accountid, product, week, avg_ratio)

    var finalResult=spark.sql(""+
      "SELECT REGIONID,PRODUCT,SUBSTR(YEARWEEK,-2) AS WEEK,AVG(RATIO)AS  RATIO" +
      " FROM MIDDLETABLE" +
      "GROUP BY REGIONID, PRODUCT, SUBSTR(YEARWEEK,5,6")


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 9. Data unloading (memory -> oracle)
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "KOPO_SPARK_ST_이동근"

    finalResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
    println("finished")
    // 다풀고 심심하면 MySql에 데이터를 저장해보세요
  }
}


