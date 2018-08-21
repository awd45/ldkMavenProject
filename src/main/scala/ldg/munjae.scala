package ldg

object munjae {

  def main(args: Array[String]): Unit = {

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.{Row, SparkSession}
    import edu.princeton.cs.introcs.StdStats
    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql.types.DoubleType

    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //    var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Function Definition ////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 이동평균 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
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

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 변동률(이동평균의 표준편차 함수 정의 )//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
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
        var resultDev = StdStats.stddev(targetData1(i)).round
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

        var mADataLast = mAIndex.
          filter(x => {
            ((x._2 >= (mALastIndex - subDevcount - index)) &&
              (x._2 <= mALastIndex))
          }).
          map(x => {
            x._1
          })
        var mADevLast = StdStats.stddev(mADataLast).round
        devpostMAArray.append(mADevLast)
      }
      var mADevResult = (devpreMAArray ++ stddevArray ++ devpostMAArray.reverse)

      mADevResult.toArray[Double]
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 상한값 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    def maxValue(mADevResult: Array[Double], MA: Array[Double]): Array[Double] = {
      var maxval = new ArrayBuffer[Double]

      var targetLen = mADevResult.size
      for (i <- 0 until targetLen) {
        var max = mADevResult(i) + MA(i)
        maxval.append(max)
      }
      maxval.toArray[Double]
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 하한값 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
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

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 정제된 qty 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    def refinedQty(realQty: Array[Double], max: Array[Double], min: Array[Double]): Array[Double] = {
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

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 스무딩 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
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
        var smavg = (smsum / 5).round
        smoothingArray.append(smavg)
      }
      for (index <- 0 until subsmcount) {
        var smDataFirst = smIndex.
          filter(x => x._2 >= 0 && x._2 <= (index + subsmcount)).
          map(x => {
            x._1
          })
        var smfirst = (smDataFirst.sum / smDataFirst.size)
        smpreMAArray.append(smfirst)
        var smDataLast = smIndex.
          filter(x => {
            ((x._2 >= (smLastIndex - subsmcount - index)) &&
              (x._2 <= smLastIndex))
          }).
          map(x => {
            x._1
          })
        var smLast = (smDataLast.sum / smDataLast.size).round
        smpostMAArray.append(smLast)
      }

      var smResult = (smpreMAArray ++ smoothingArray ++ smpostMAArray.reverse)

      smResult.toArray[Double]
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 계절성지수 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    def seasonalityzisu (refindQty : Array[Double], smoothing : Array[Double]) : Array[Double] ={
      var seasonalArray = new ArrayBuffer[Double]

      for(i <- 0 until refindQty.size){

        var result = refindQty(i)/smoothing(i)
        seasonalArray.append(result)
      }
      seasonalArray.toArray[Double]
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 데이터로딩 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    def dataLoad(sqlSelect : String ,id : String , password : String, tableName : String, ipAsPortAsserviceName :String , tempName : String
                ) : org.apache.spark.sql.DataFrame  = {
      var staticUrl = ""
      var staticUser = id
      var staticPw = password
      var selloutDb = tableName
      if (sqlSelect == "oracle") {
        staticUrl = "jdbc:oracle:thin:@" + ipAsPortAsserviceName
      }else {
        staticUrl = "jdbc:"+sqlSelect+"://" + ipAsPortAsserviceName
      }
      var selloutData = spark.read.format("jdbc").
        options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

      selloutData.createOrReplaceTempView(tempName)

      println(selloutData.show())
      selloutData

    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////// 데이터저장 함수 정의//////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    def dataOut(sqlSelect : String ,id : String , password : String, outTableName : String, ipAsPortAsserviceName :String , dfName : org.apache.spark.sql.DataFrame) = {
      var outputUrl = ""
      var outputUser = id
      var outputPw = password
      val prop = new java.util.Properties
      if (sqlSelect == "oracle") {
        outputUrl = "jdbc:oracle:thin:@" + ipAsPortAsserviceName
        prop.setProperty("driver", "oracle.jdbc.OracleDriver")
      }else if(sqlSelect == "postgresql" ) {
        outputUrl = "jdbc:"+sqlSelect+"://" + ipAsPortAsserviceName
        prop.setProperty("driver", "org.postgresql.Driver")}
      else{
        outputUrl = "jdbc:"+sqlSelect+"://" + ipAsPortAsserviceName
        prop.setProperty("driver", "com.mysql.jdbc.Driver")
      }
      prop.setProperty("user", outputUser)
      prop.setProperty("password", outputPw)
      val table = outTableName
      dfName.write.mode("overwrite").jdbc(outputUrl, table, prop)
      println("finished")

    }


    // 1. data loading
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var kopoDf = dataLoad("oracle","kopo","kopo","kopo_channel_seasonality_new","192.168.110.111:1521/orcl","keydata")


    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////


    var rawData = spark.sql("select " +
      "a.regionid as accountid," +
      "a.product," +
      "a.yearweek," +
      "cast(case When a.qty <= 0 then 1 else a.qty end as String) as qty, " + //qty 음수와 0값을 1로 처리
      "'test' as productname from keydata a" )

    rawData.show(2)

    var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})

    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    // 1~52주차 데이터뽑기
    var filterEx1Rdd = rawRdd.filter(x=> {

      var checkValid = true
      var week = x.getString(yearweekNo).substring(4, 6).toInt
      var standardWeek = 52
      if(week > standardWeek){
        checkValid = false
      }
      checkValid
    })

    var filterRdd = filterEx1Rdd

    // 필터링한 데이터를 map함수로 데이터값을 변경
    // qtyNo가 700000 이상이면 700000으로 값을 가져온다.
    var mapRdd = filterRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var week = (x.getString(yearweekNo)).substring(4,6)
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row(
        x.getString(accountidNo),
        x.getString(productNo),
        x.getString(yearweekNo),
        qty,
        x.getString(productnameNo))
    })

    // 위의 정제된 RDD를 그룹핑한다.
    var groupRddMapExp = mapRdd.
      groupBy(x=>{ (x.getString(accountidNo),x.getString(productNo))}).
      flatMap(x=>{

        // key = (A21_PRODUCT1,A21)
        // data = CompactBuffer([A21_PRODUCT1,A21,PRODUCT1,201401,0.0,test],.... 많이들어간다.
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
        var jungjae =refinedQty(qty,maxValueResult,minValueResult)
        var smoothingResult = smoothing(jungjae)
        var seasonalityResult = seasonalityzisu(jungjae,smoothingResult)

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
            var ratio = 1.0d
            var stdevMa = x._2
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
        finalResult2

        var finalResult3 = finalResult2.
          zip(maxValueResult).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1(6)
            var maxValueResult = x._2
            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa ,
              maxValueResult
            )
          })
        finalResult3

        var finalResult4 = finalResult3.
          zip(minValueResult).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1(6)
            var maxValueResult = x._1(7)
            var minValueResult = x._2
            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            Row(regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa ,
              maxValueResult,
              minValueResult
            )
          })
        finalResult4

        var finalResult5 = finalResult4.
          zip(jungjae).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1.getDouble(6)
            var maxValueResult = x._1(7)
            var minValueResult = x._1(8)
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
              maxValueResult,
              minValueResult,
              jungjaegood
            )
          })
        finalResult5

        var finalResult6 = finalResult5.
          zip(smoothingResult).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1.getDouble(6)
            var maxValueResult = x._1.getDouble(7)
            var minValueResult = x._1.getDouble(8)
            var jungjaegood = x._1.getDouble(9)
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
              maxValueResult,
              minValueResult,
              jungjaegood,
              smoothingnie
            )
          })
        finalResult6

        var finalResult7 = finalResult6.
          zip(seasonalityResult).
          map(x=>{
            var regionid = x._1.getString(0)
            var product = x._1.getString(1)
            var yearweek = x._1.getString(2).substring(4,6)
            var volume = x._1.getDouble(3)
            var movingValue = x._1.getDouble(4)
            var ratio = 1.0d
            var stdevMa = x._1.getDouble(6)
            var maxValueResult = x._1.getDouble(7)
            var minValueResult = x._1.getDouble(8)
            var jungjaegood = x._1.getDouble(9)
            var smoothingnie = x._1.getDouble(10)
            var seasonality = x._2
            if(movingValue != 0.0d){
              ratio = volume / movingValue
            }
            (regionid,
              product,
              yearweek,
              volume,
              movingValue,
              ratio,
              stdevMa,
              maxValueResult,
              minValueResult,
              jungjaegood,
              smoothingnie,
              seasonality
            )
          })
        finalResult7
      })

    var middleResult2 = groupRddMapExp.toDF("REGIONID","PRODUCT","WEEK","VOLUME","MOVINGVALUE","RATIO","STDEVMA","MAXVALUE","MINVALUE","REFINEDQTY","SMOOTHINGNIE","SEASONALITY")

    middleResult2.createOrReplaceTempView("middleTable")

    var finalResultDf = spark.sql("select REGIONID,PRODUCT,week,round(avg(VOLUME),6) as QTY,round(avg(MOVINGVALUE),6) as MOVINGVALUE,round(avg(RATIO),6) as RATIO,round(avg(STDEVMA),6) as STDEV_MA,round(avg(MAXVALUE),6) as MAXVALUE,round(avg(MINVALUE),6) as MINVALUE" +
      "  ,round(avg(REFINEDQTY),6) as REFINED_QTY,round(avg(SMOOTHINGNIE),6) as SMOOTHING,round(avg(SEASONALITY),6) as SEASONALITY from middleTable group by REGIONID,PRODUCT,WEEK"
      +" order by REGIONID, PRODUCT, WEEK")

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Data Uploading        ////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    dataOut("postgresql","kopo","kopo","ohmyg","192.168.110.111:5432/kopo",finalResultDf)

    println("Seasonality model completed, Data Inserted in Oracle DB")
  }
}


