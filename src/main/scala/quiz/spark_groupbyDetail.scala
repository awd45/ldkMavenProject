package quiz

object spark_groupbyDetail {

    def main(args: Array[String]): Unit = {
      import edu.princeton.cs.introcs.StdStats
      //https://introcs.cs.princeton.edu/java/22library/StdStats.java.html
      import org.apache.spark.sql.SparkSession
      import scala.collection.mutable.ArrayBuffer
      import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
      import org.apache.spark.sql.types.{StringType, StructField, StructType}

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
      var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // Path setting
      //    var dataPath = "./data/"
      //    var mainFile = "kopo_channel_seasonality_ex.csv"
      //    var subFile = "kopo_product_master.csv"
      //
      //    var path = "c:/spark/bin/data/"
      //
      //    // Absolute Path
      //    //kopo_channel_seasonality_input
      //    var mainData = spark.read.format("csv").option("header", "true").load(path + mainFile)
      //    var subData = spark.read.format("csv").option("header", "true").load(path + subFile)
      //
      //    spark.catalog.dropTempView("maindata")
      //    spark.catalog.dropTempView("subdata")
      //    mainData.createTempView("maindata")
      //    subData.createOrReplaceTempView("subdata")
      //
      //    /////////////////////////////////////////////////////////////////////////////////////////////////////
      //    ////////////////////////////////////  Data Refining using sql////////////////////////////////////////
      //    /////////////////////////////////////////////////////////////////////////////////////////////////////
      //    var joinData = spark.sql("select a.regionid as accountid," +
      //      "a.product as product, a.yearweek, a.qty, b.productname " +
      //      "from maindata a left outer join subdata b " +
      //      "on a.productgroup = b.productid")
      //
      //    joinData.createOrReplaceTempView("keydata")
      // 1. data loading
      //////////////////////////////////////////////////////////////////////////////////////////////////
      var staticUrl = "jdbc:oracle:thin:@192.168.0.10:1521/XE"
      staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
      var staticUser = "kopo"
      var staticPw = "kopo"
      var selloutDb = "kopo_channel_seasonality_new"

      val  selloutDataFromOracle = spark.read.format("jdbc").
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
      //      "cast(targetas double) as target," +
      //      "idx from selloutTable where 1=1"
      var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
        "a.regionid as accountid, " +
        "a.product, " +
        "a.yearweek, " +
        "cast(a.qty as String) as qty, " +
        "'test' as productname from keydata a" +
        " where 1=1" +
        " and regionid = 'A01' " +
        " and product in ('PRODUCT1','PRODUCT2') " )

      rawData.show(2)

      var rawDataColumns = rawData.columns
      var keyNo = rawDataColumns.indexOf("keycol")
      var accountidNo = rawDataColumns.indexOf("accountid")
      var productNo = rawDataColumns.indexOf("product")
      var yearweekNo = rawDataColumns.indexOf("yearweek")
      var qtyNo = rawDataColumns.indexOf("qty")
      var productnameNo = rawDataColumns.indexOf("productname")

      var rawRdd = rawData.rdd

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // The abnormal value is refined using the normal information
      var filterRdd = rawRdd.filter(x=>{

        // Data comes in line by line
        var checkValid = true
        // Assign yearweek information to variables
        var week = x.getString(yearweekNo).substring(4,6).toInt
        // Assign abnormal to variables
        var standardWeek = 52

        // filtering
        if (week > standardWeek)
        {
          checkValid = false
        }
        checkValid
      })

      // key, account, product, yearweek, qty, productname
      var mapRdd = filterRdd.map(x=>{
        var qty = x.getString(qtyNo).toDouble
        var maxValue = 700000
        if(qty > 700000){qty = 700000}
        Row( x.getString(keyNo),
          x.getString(accountidNo),
          x.getString(productNo),
          x.getString(yearweekNo),
          qty, //x.getString(qtyNo),
          x.getString(productnameNo))
      })

      var groupRddMapAnswer2 = mapRdd.
        groupBy(x=>{ (x.getString(keyNo)) }).
        // map/flatmap
        flatMap(x=>{
        // 기본 데이터 설정
        var key = x._1
        var data = x._2
        var size = x._2.size
        var qty = x._2.map(x=>{x.getDouble(qtyNo)}).toArray

        // 평균, 표준편차 계산
        var average = StdStats.mean(qty)
        var stddev = StdStats.stddev(qty)

        // 결과 출력
        var mapResult = data.map(x=>{
          (key,
            size,
            average,
            stddev)
        })
        mapResult
      })

      var groupRddMapAnswer1 = mapRdd.
        groupBy(x=>{ (x.getString(keyNo)) }).
        // map/flatmap
        map(x=>{
        // 기본 데이터 설정
        var key = x._1
        var data = x._2
        var size = x._2.size
        var qty = x._2.map(x=>{x.getDouble(qtyNo)}).toArray

        // 평균, 표준편차 계산
        var average = StdStats.mean(qty)
        var stddev = StdStats.stddev(qty)

        (key,(size,average,stddev))
      })

      var groupRddMapAnswer3 = mapRdd.
        groupBy(x=>{ (x.getString(keyNo)) }).
        // map/flatmap
        map(x=>{
        // 기본 데이터 설정
        var key = x._1
        var data = x._2
        var size = x._2.size
        var qty = x._2.map(x=>{x.getDouble(qtyNo)}).toArray

        // 평균, 표준편차 계산
        var average = StdStats.mean(qty)
        var stddev = StdStats.stddev(qty)

        // 결과 출력
        var mapResult = data.map(x=>{
          (key,
            size,
            average,
            stddev)
        })
        mapResult
      })

    }
    //        var size = x._2.map(x=>{x.getDouble(qtyNo)}).size
    //        var sumation = x._2.map(x=>{x.getDouble(qtyNo)}).sum
    //        var average = 0.0d
    //        if(size!=0){
    //          average = sumation/size
    //        }else{
    //          average = 0
    //        }
    //        // Calculate 분산
    //        var variance = x._2.map(x=>{math.pow((x.getDouble(qtyNo)-average),2)}).sum/(size-1)  ///average
    //        // Calculate 표준편차
    //        var stddev = math.sqrt(variance)
    //        var mapResult = data.map(x=>{
    //          (key,
    //            size,
    //            average,
    //            stddev)
    //        })
    //        mapResult


  }



