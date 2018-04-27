 //중간고사 정다! 포인트 sparksql을 쓰면 필터와 맵 함수를 동시에 쓸 수 있다.!!
 //교수님의 의도!!

  package MiddleTest


  object middleTest_answer {

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
        "and substring(yearweek, 5, 6) != " + INVALIDWEEK +
        "and product in ("+PRODUCTLIST+") ")
      rawData.show(2)
      ////// 데이터베이스 접속 및 분석결과 출력
      val postUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
      val prop2 = new java.util.Properties
      prop2.setProperty("driver", "org.postgresql.Driver")
      prop2.setProperty("user", staticUser)
      prop2.setProperty("password", staticPw)
      rawData.write.mode("overwrite").jdbc(postUrl, "outResult", prop2)









      //dataframe을 Rdd로 변환
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

