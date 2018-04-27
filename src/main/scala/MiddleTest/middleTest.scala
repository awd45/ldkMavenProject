package MiddleTest

import org.apache.spark.sql.SparkSession

object middleTest {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("hkProject").

      config("spark.master", "local").

      getOrCreate()



    // 1번문제 [로딩] poly_server2 (192.168.110.112) kopo 스키마에서 kopo_channel_seasonality_new 자료를 불러와서 데이터프레임에
    // 저장한 후 2줄을 show 할 수 있는 코드를 작성한 후 show하는 코드에 주석처리만 하세요
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

    var staticUser = "kopo"

    var staticPw = "kopo"

    var selloutDb = "kopo_channel_seasonality_new"

    // jdbc (java database connectivity) 연결

    val selloutDf = spark.read.format("jdbc").

      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성

    selloutDf.createOrReplaceTempView("selloutTable")

    //1번 답
    selloutDf.show(2)


    //2번 답 [SQL분석] kopo_channel_seasonality_new의 qty값을 sparksql을 활용하여
    // 1.2배 증가시킨 후 qty_new컬럼을 추가로 생성하는 코드를 작성한 후 코드에 주석처리만 하세요
    var middleResult = spark.sql("select regionid, product, yearweek, cast(qty as double), cast(QTY * 1.2 as double)AS QTY_NEW FROM selloutTable")


    //3번 이클립스로 함수 만들기

    //4번답 [정제] 2016년도 이상, 52주차 미포함, 프로덕트 정보가 (PRODUCT1, PRODCUT2)인 데이터만 남기는 코드를
    //작성한 후 코드시작부분에 주석처리만 하세요
    var rawData = middleResult
    var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})

    var accountidNo = rawDataColumns.indexOf("regionid")

    var productNo = rawDataColumns.indexOf("product")

    var yearweekNo = rawDataColumns.indexOf("yearweek")

    var qtyNo = rawDataColumns.indexOf("qty")

    var qtyNewNo = rawDataColumns.indexOf("qty_new")


    //Rdd로 변환
    var rawRdd = rawData.rdd

    var x = rawRdd.first

    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = false;
      var yearValue = x.getString(yearweekNo).substring(0,4).toInt

      if ( ((x.getString(productNo) == "PRODUCT1") ||
            (x.getString(productNo) == "PRODUCT2")) &&
            (yearValue >= 2016)) {
        checkValid = true;
      }
      checkValid
    })

    var filteredRdd2 = filteredRdd.filter(x=>{
      var checkValid = true;
      var weekValue = x.getString(yearweekNo).substring(4).toInt
      if (weekValue ==52){
        checkValid = false;

      }

      checkValid

    })



    //5번 [저장] 실습한 코드를 Prostgresql(192.168.110.111) 서버에 kopo_st_result_ldg 한 후 전체 코드를 "이동근_전체코드.scala"
    //파일을 메일로 첨부하세요
    import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

    val finalResultDf = spark.createDataFrame(filteredRdd2,

      StructType(

        Seq(
          StructField("regionid", StringType),

          StructField("product", StringType),

          StructField("yearweek", StringType),

          StructField("qty", DoubleType),

          StructField("qty_new", DoubleType))))

    finalResultDf.createOrReplaceTempView("finalResultDfTb")
    finalResultDf.show(2)


    var myUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var outputUser = "kopo"
    var outputPw = "kopo"

    val prop = new java.util.Properties

    prop.setProperty("driver", "org.postgresql.Driver")

    prop.setProperty("user", "kopo")

    prop.setProperty("password", "kopo")

    val table = "kopo_st_result_ldg"

    finalResultDf.write.mode("overwrite").jdbc(myUrl, table, prop)


    //6.[시각화] 저장한 데이터를 oracle_visualization을 활용하여 상품별 거래량 정보가 시간 정보에 따라 라인 차트로 시각화 도디도록
    //구성하여 캡쳐한 후 "이동근_6번답.jpg" 메일로 첨부하세요

    //7.[보너스] KOSPI지수 나타내기
    // 답은 innerjoin




    //중간고사 정다! 포인트 sparksql을 쓰면 필터와 맵 함수를 동시에 쓸 수 있다.!!

    package MiddleTest

    object Seasonality_ex {

      def main(args: Array[String]): Unit = {

        /////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////  Library Definition ////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////
        import org.apache.spark.sql.SparkSession

        import scala.collection.mutable.ArrayBuffer
        import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
        import org.apache.spark.sql._
        import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////  Function Definition ////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        // Function: Return movingAverage result
        // Input:
        //   1. Array : targetData: inputsource
        //   2. Int   : myorder: section
        // output:
        //   1. Array : result of moving average
        def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
          val length = targetData.size
          if (myorder > length || myorder <= 2) {
            throw new IllegalArgumentException
          } else {
            var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

            if (myorder % 2 == 0) {
              maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
            }
            maResult.toArray
          }
        }

        ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
        var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

        var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
        var staticUser = "kopo"
        var staticPw = "kopo"
        var selloutDb = "kopo_channel_seasonality_new"
        ////// 데이터베이스 접속 및 분석데이터 로딩
        val selloutDataFromOracle = spark.read.format("jdbc").
          options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
        selloutDataFromOracle.createOrReplaceTempView("keydata")
        ////// 로직 구현 (2016년도 이상 / 52주차제거 / 관심상품만 등록)
        var VALIDYEAR = "2016"
        var INVALIDWEEK = 52
        var PRODUCTLIST = "'PRODUCT1','PRODUCT2'"
        var rawData = spark.sql("select regionid, " +
          "product," +
          "yearweek," +
          "cast(qty as Double) as qty, " +
          "cast(qty * 1.2 as Double) as new_qty from keydata a " +
          "where 1=1 " +
          "and substring(yearweek, 0, 4) >= " + VALIDYEAR +
          " and substring(yearweek, 5, 6) != " + INVALIDWEEK +
          " and product in ("+PRODUCTLIST+") ")
        rawData.show(2)
        ////// 데이터베이스 접속 및 분석결과 출력
        val postUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
        val prop2 = new java.util.Properties
        prop2.setProperty("driver", "org.postgresql.Driver")
        prop2.setProperty("user", staticUser)
        prop2.setProperty("password", staticPw)
        rawData.write.mode("overwrite").jdbc(postUrl, "outResult", prop2)


















        var rawDataColumns = rawData.columns.map(x => {
          x.toLowerCase()
        })
        var accountidNo = rawDataColumns.indexOf("regionid")
        var productNo = rawDataColumns.indexOf("product")
        var yearweekNo = rawDataColumns.indexOf("yearweek")
        var qtyNo = rawDataColumns.indexOf("qty")
        var productnameNo = rawDataColumns.indexOf("new_qty")

        var paramDb = "kopo_parameter_hk"

        val paramDataFromOracle = spark.read.format("jdbc").
          options(Map("url" -> staticUrl, "dbtable" -> paramDb, "user" -> staticUser, "password" -> staticPw)).load

        paramDataFromOracle.createOrReplaceTempView("paramdata")
        val parametersDF = spark.sql("select * from paramdata where use_yn = 'Y'")

        var paramDataColumns = parametersDF.columns.map(x => {
          x.toLowerCase()
        })
        var pCategoryNo = paramDataColumns.indexOf("param_category")
        var pNameNo = paramDataColumns.indexOf("param_name")
        var pValueNo = paramDataColumns.indexOf("param_value")

        // Define Map
        val parameterMap =
          parametersDF.rdd.groupBy {x => (x.getString(pCategoryNo), x.getString(pNameNo))}.map(row => {
            var paramValue = row._2.map(x=>{x.getString(pValueNo)}).toArray
            ( (row._1._1, row._1._2), (paramValue) )
          }).collectAsMap

        //var productSet = .mkString(",").split(",")
        var PRODUCTSET = new Array[String](0).toSet //selloutDataFromOracle.rdd.map(x => { x.getString(productNo) }).distinct().collect.toSet
        if ( parameterMap.contains("COMMON","VALID_PRODUCT") ) {
          PRODUCTSET = parameterMap("COMMON","VALID_PRODUCT").toSet
        }
        var VALIDWEEK = 53
        if ( parameterMap.contains("COMMON","VALID_WEEK") ) {
          VALIDWEEK = parameterMap("COMMON","VALID_WEEK")(0).toInt
        }
        var VALIDYEAR = 2016
        if ( parameterMap.contains("COMMON","VALID_YEAR") ) {
          VALIDYEAR = parameterMap("COMMON","VALID_YEAR")(0).toInt
        }

        var rawRdd = rawData.rdd

        def getWeekInfo(inputData: String): Int = {

          var answer = inputData.substring(4, 6).toInt
          answer
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        // The abnormal value is refined using the normal information

        var filterRdd = rawRdd.filter(x => {
          var checkValid = false

          var yearInfo = x.getString(yearweekNo).substring(0, 4).toInt
          var weekInfo = getWeekInfo(x.getString(yearweekNo))

          if ((weekInfo < VALIDWEEK) &&
            (yearInfo >= VALIDYEAR) &&
            (PRODUCTSET.contains(x.getString(productNo)))) {
            var checkValid = true
          }
          checkValid
        })

        ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        val finalResult = spark.createDataFrame(filterRdd,
          StructType(
            Seq(
              StructField("regionid", StringType),
              StructField("product", StringType),
              StructField("yearweek", StringType),
              StructField("volume", DoubleType),
              StructField("product_name", DoubleType))))


        ///Oracle
        val prop = new java.util.Properties
        prop.setProperty("driver", "oracle.jdbc.OracleDriver")
        prop.setProperty("user", staticUser)
        prop.setProperty("password", staticPw)
        val table = "kopo_st_result"

        var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

        finalResult.write.mode("overwrite").jdbc(outputUrl, table, prop)
        println("Seasonality model completed, Data Inserted in Oracle DB")

        // jdbc (java database connectivity) 연결
        finalResult.write.mode("overwrite").jdbc(postUrl, table, prop2)

        ///mysql
        val myUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
        val prop3 = new java.util.Properties
        prop3.setProperty("driver", "com.mysql.jdbc.Driver")
        prop3.setProperty("user", "root")
        prop3.setProperty("password", "P@ssw0rd")

        // jdbc (java database connectivity) 연결
        finalResult.write.mode("overwrite").jdbc(myUrl, table, prop3)

      }
    }

  }
}
