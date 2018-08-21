package 실습

object last {

  def main(args: Array[String]): Unit = {

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.{Row, SparkSession}
    import edu.princeton.cs.introcs.StdStats
    import scala.collection.mutable.ArrayBuffer

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Function Definition ////////////////////////////////////
    // Function: Return movingAverage result
    // Input:
    //   1. Array : targetData: inputsource
    //   2. Int   : myorder: section
    // output:
    //   1. Array : result of moving average
    // 이동평균 함수 정의
    def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(x=>{x.sum}).map(x=>{ x/myorder })

        if (myorder % 2 == 0) {
          maResult = maResult.sliding(2).map(x=>{x.sum}).map(x=>{ x/2 })
        }
        maResult.toArray
      }
    }


    def stdev(targetData: Array[Double], myorder: Int): Array[Double] = {
      var stddevArray = new ArrayBuffer[Double]
      var devpreMAArray = new ArrayBuffer[Double]
      var devpostMAArray = new ArrayBuffer[Double]
      var mAIndex = targetData.zipWithIndex
      var mALastIndex = mAIndex.size - 1
      var subDevcount = (myorder.toDouble / 2.0).floor.toInt

      var targetData1 = targetData.sliding(myorder).toArray

      var length = targetData1.size
      for (i <- 0 until length) {
        var resultDev = StdStats.stddev(targetData1(i))
        stddevArray.append(resultDev)

      }
      for (index <- 0 until subDevcount) {
        var mADataFirst = mAIndex.
          filter(x => x._2 >= 0 && x._2 <= (index + subDevcount)).
          map(x => {
            x._1
          })
        var mADevfirst = StdStats.stddev(mADataFirst)
        devpreMAArray.append(mADevfirst)
        ////////////////////////////////////////////////////////
        //////////////////이동표준편차뒷부분채우기 //////////////////
        var mADataLast = mAIndex.
          filter(x => {
            ((x._2 >= (mALastIndex - subDevcount - index)) &&
              (x._2 <= mALastIndex))
          }).
          map(x => {
            x._1
          })
        var mADevLast = StdStats.stddev(mADataLast)
        devpostMAArray.append(mADevLast)
      }

      var mADevResult = (devpreMAArray ++ stddevArray ++ devpostMAArray.reverse)

      mADevResult.toArray[Double]
    }

    //var aa =stdev(targetData1,myorder1)
    ////////////////// //////// 상한가 구하기


    def maxValue(mADevResult: Array[Double], MA: Array[Double]): Array[Double] = {
      var maxval = new ArrayBuffer[Double]

      var targetLen = mADevResult.size
      for (i <- 0 until targetLen) {
        var max = mADevResult(i) + MA(i)
        maxval.append(max)
      }
      maxval.toArray[Double]
    }

    ///////////////////////////하한가 구한기
    def minValue(mADevResult: Array[Double], MA: Array[Double]): Array[Double] = {

      var minval = new ArrayBuffer[Double]
      var targetLen = mADevResult.size
      for (i <- 0 until targetLen) {
        var max = mADevResult(i) + MA(i)
        var min = mADevResult(i) - MA(i)
        minval.append(min)
      }
      minval.toArray[Double]
    }



    //아래꺼 정재한값구하기
    //var realQty =Array[Double](0,10,10,10,10)
    // var max = Array[Double](8,9,10,11,12)
    //var min = Array[Double](5,6,7,8,3)
    def beegu(realQty: Array[Double], max: Array[Double], min: Array[Double]): Array[Double] = {
      var beeguArray = new ArrayBuffer[Double]
      var length = realQty.size
      for (i <- 0 until length) {
        if (realQty(i) >= max(i)) {
          max(i)
          beeguArray.append(max(i))
        } else if (realQty(i) <= min(i)) {
          min(i)
          beeguArray.append(min(i))
        } else {
          realQty(i)
          beeguArray.append(realQty(i))
        }
      }
      beeguArray.toArray[Double]
    }

    //////////////////////스무딩처리하기 qty/5
    var beeguArray = Array(5.5, 9.3, 4.4, 5.3, 7.7, 2.2)

    def smoothing(beeguArray: Array[Double]): Array[Double] = {

      var smoothingArray = new ArrayBuffer[Double]
      var smpreMAArray = new ArrayBuffer[Double]
      var smpostMAArray = new ArrayBuffer[Double]
      var smIndex = beeguArray.zipWithIndex
      var smLastIndex = smIndex.size - 1
      var subsmcount = (5.toDouble / 2.0).floor.toInt

      var smoothingData = beeguArray.sliding(5).toArray

      var length = smoothingData.size
      for (i <- 0 until length) {
        var smsum = smoothingData(i).sum
        var smavg = smsum / 5
        smoothingArray.append(smavg)
      }
      for (index <- 0 until subsmcount) {
        var smDataFirst = smIndex.
          filter(x => x._2 >= 0 && x._2 <= (index + subsmcount)).
          map(x => {
            x._1
          })
        //이거 반올림해야되는데 라운드어캐쓸까염
        var smfirst = smDataFirst.sum / smDataFirst.size
        smpreMAArray.append(smfirst)
        ////////////////////////////////////////////////////////
        //////////////////이동표준편차뒷부분채우기 //////////////////
        var smDataLast = smIndex.
          filter(x => {
            ((x._2 >= (smLastIndex - subsmcount - index)) &&
              (x._2 <= smLastIndex))
          }).
          map(x => {
            x._1
          })
        var smLast = smDataLast.sum / smDataLast.size
        smpostMAArray.append(smLast)
      }

      var smResult = (smpreMAArray ++ smoothingArray ++ smpostMAArray.reverse)

      smResult.toArray[Double]
    }
    // myorder는 이동평균날개범위
    // 2이하는 이동평균의미가 없다 || 날개가 타깃데이터갯수 보다 크면 예외처리

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
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    //staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("keydata")

    println(selloutDataFromOracle.show())
    println("oracle ok")

    // kopo_channel_seasonality_new 데이터 불와서 테이블로 만든다.

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
    //      "salesid, salesname, productgroup, product, item," +
    //      "yearweek, year, week, " +
    //      "cast(qty as double) as qty," +
    //      "cast(target as double) as target," +
    //      "idx from selloutTable where 1=1"

    // keycol은 나중에 파라미터만 키값변경,추가하면 따로 코드수정이 필요하지않음 key값만 주면됌
    var rawData = spark.sql("select " +
      "a.regionid as accountid," +
      "a.product," +
      "a.yearweek," +
      "cast(a.qty as String) as qty, " +
      "'test' as productname from keydata a" )

    //테이블을 원하는 테이블로 정제한다.
    rawData.show(2)

    var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})

    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")
    // 인덱스를 고정히켜준다.

    var rawRdd = rawData.rdd
    // RDD로 만들어 준다.

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // The abnormal value is refined using the normal information

    // RDD를 원하는 조건으로 필터링 해준다.
    // 1~52주차 데이터뽑기
    var filterEx1Rdd = rawRdd.filter(x=> {

      // Data comes in line by line
      var checkValid = true
      // Assign yearweek information to variables
      var week = x.getString(yearweekNo).substring(4, 6).toInt
      // Assign abnormal to variables
      var standardWeek = 52

      if(week > standardWeek){
        checkValid = false
      }
      checkValid
    })

    //   // PRODUCT1 PRODUCT2 를 포함하는 데이터만 뽑기
    //    var productList = Array("PRODUCT1","PRODUCT2").toSet
    //      .filter(x=> {
    //        productList.contains(x.getString(productNo))
    //      })
    var filterRdd = filterEx1Rdd

    // 필터링한 데이터를 map함수로 데이터값을 변경
    // qtyNo가 700000 이상이면 700000으로 값을 가져온다.
    // key, account, product, yearweek, qty, productname
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var week = (x.getString(yearweekNo)).substring(4,6)
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row(
        x.getString(accountidNo),
        x.getString(productNo),
        //        x.getString(yearweekNo),
        week.toString,
        qty, //x.getString(qtyNo),
        x.getString(productnameNo))
    })

    /////////////////////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Processing        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Distributed processing for each analytics key value (regionid, product)
    // 1. Sort Data
    // 2. Calculate moving average
    // 3. Generate values for week (out of moving average)
    // 4. Merge all data-set for moving average
    // 5. Generate final-result
    // (key, account, product, yearweek, qty, productname)

    // 위의 정제된 RDD를 그룹핑한다.
    var groupRddMapExp5 = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),x.getString(productNo))}).
      flatMap(x=>{

        // key = (A21_PRODUCT1,A21)
        // data = CompactBuffer([A21_PRODUCT1,A21,PRODUCT1,201401,0.0,test], ..엄청많이 들어감
        var key = x._1
        var data = x._2

        // 표준편차구하기
        var size = x._2.size
        var qty = x._2.map(x=>{x.getDouble(qtyNo)}).toArray

        // 1. Sort Data
        var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt}) // 연주차 오름차순으로 정렬
        var sortedDataIndex = sortedData.zipWithIndex //데이터값에 인덱스추가

        var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNo))}).toArray // qtyNo만 가져옴 (156개 데이터값)
        var sortedVolumeIndex = sortedVolume.zipWithIndex // 인덱스값이 추가됌 ((0.0,0), (0.0,1)......)

        // 꼭 홀수값만 입력받아야 자기자신값 제외하고 양쪽날개 짝수로 생김
        var scope = 17 //날개 범위 17개
        var subScope = (scope.toDouble / 2.0).floor.toInt // 반쪽날개 범위8개

        // 2. 이동편균함수를 써서 계산한다.
        var movingResult = movingAverage(sortedVolume,scope)

        // 3. Generate value for weeks (out of moving average)
        //    이동평균의 예외범위의 데이터값을 구한다.
        //    scope는 날개 subscope은 날개의 반
        var preMAArray = new ArrayBuffer[Double]
        var postMAArray = new ArrayBuffer[Double]

        // 마지막인덱스는 156
        // for문 디버깅할때는 index값을 임의로 넣어주고 디버깅
        // 임의로 index에 7을 넣어 디버깅
        // 이동평균 앞부분 채우기
        var lastIndex = sortedVolumeIndex.size-1
        for (index <- 0 until subScope) {
          var scopedDataFirst = sortedVolumeIndex.
            filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
            map(x => {
              x._1
            })
          var scopedSum = scopedDataFirst.sum
          var scopedSize = scopedDataFirst.size
          var scopedAverage = scopedSum / scopedSize
          preMAArray.append(scopedAverage) // scopedAverage값 0.0이 16개까지 append된다!

          // 이동평균 뒷부분 채우기
          // for문 디버깅할때는 index값을 임의로 넣어주고 디버깅
          var scopedDataLast = sortedVolumeIndex.
            filter(x => {
              ((x._2 >= (lastIndex - subScope - index)) &&
                (x._2 <= lastIndex))
            }).
            map(x => {
              x._1
            })
          var secondScopedSum = scopedDataLast.sum
          var secondScopedSize = scopedDataLast.size
          var secondScopedAverage = secondScopedSum / secondScopedSize
          postMAArray.append(secondScopedAverage)
        }

        // 4. Merge all data-set for moving average
        var maResult = (preMAArray++movingResult++postMAArray.reverse).toArray
        var maResultIndex = (preMAArray++movingResult++postMAArray.reverse).zipWithIndex
        var stdevMaIndex = stdev(maResult,5).zipWithIndex
        var stdevMa = stdev(maResult,5)
        var maxValueResult = maxValue(maResult,stdevMa)
        var minValueResult = minValue(maResult,stdevMa)
        var jungjae = beegu(qty,maxValueResult,minValueResult)
        var smoothingResult = smoothing(jungjae)

        // 5. Generate final-result
        var finalResult = sortedData.
          zip(maResult).
          map(x=>{

            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._2
            var ratio = 1.0d
            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }

            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio)
          })
        finalResult
        // 스무딩 구하기
        // 정제된Qty를 가지고 이동평균 구하는것
        var finalResult2 = finalResult.
          zip(stdevMa).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var stdevMa = x._2
            var ratio = 1.0d
            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa
            )
          })
        finalResult2  // 스무딩 구하기
        // 정제된Qty를 가지고 이동평균 구하는것
        var finalResult3 = finalResult2.
          zip(jungjae).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1.getDouble(6)
            var jungjaegood = x._2

            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa,
              jungjaegood

            )
          })
        finalResult3
        var finalResult4 = finalResult3.
          zip(smoothingResult).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1.getDouble(6)
            var jungjaegood = x._1.getDouble(7)
            var smoothingnie = x._2

            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa,
              jungjaegood,
              smoothingnie
            )
          })
        finalResult4
      })








    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////


    val middleResult = spark.createDataFrame(groupRddMapExp5,
      StructType(Seq(StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("WEEK", StringType),
        StructField("VOLUME", StringType),
        StructField("MA", StringType),
        StructField("RATIO", StringType),
        StructField("AVERAGE", StringType),
        StructField("STDDEV", StringType),
        StructField("UPBOUND", StringType),
        StructField("DOWNBOUND", StringType),
        StructField("REFINEDQTY", StringType),
        StructField("SMOOTHHANDLING", StringType))))

    middleResult.createOrReplaceTempView("middleTable")
    // 데이터프레임을 디버깅하고 .show()로 본다.

    var finalResultDf = spark.sqlContext.sql("select REGIONID, PRODUCT, WEEK, VOLUME, AVG(MA) AS AVG_MA," +
      " AVG(RATIO) AS AVG_RATIO, AVERAGE, STDDEV, UPBOUND, DOWNBOUND, REFINEDQTY, SMOOTHHANDLING from middleTable group by REGIONID, PRODUCT, WEEK, AVERAGE, STDDEV, UPBOUND, DOWNBOUND, VOLUME, REFINEDQTY, SMOOTHHANDLING")

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Unloading        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // File type
    //    finalResultDf.
    //      coalesce(1). // 파일개수
    //      write.format("csv").  // 저장포맷
    //      mode("overwrite"). // 저장모드 append/overwrite
    //      save("season_result") // 저장파일명
    //    println("spark test completed")
    // Database type
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "abcdefgh123"

    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"

    finalResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("Seasonality model completed, Data Inserted in Oracle DB")
  }
}
