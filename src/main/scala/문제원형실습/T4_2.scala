package 문제원형실습

object T4_2 {

  import java.lang.Math

  import edu.princeton.cs.introcs.StdStats
  import org.apache.spark.sql.{Row, SparkSession}

  import scala.Predef.genericWrapArray
  import scala.collection.mutable.ArrayBuffer
  import scala.math._

    def main(args: Array[String]): Unit = {

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Library Definition ////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      import org.apache.spark.sql.SparkSession
      import scala.collection.mutable.ArrayBuffer
      import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
      import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType}
      import edu.princeton.cs.introcs.StdStats
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
          var maResult = targetData.sliding(myorder).map(x=>{x.sum}).map(x=>{ x/myorder })

          if (myorder % 2 == 0) {
            maResult = maResult.sliding(2).map(x=>{x.sum}).map(x=>{ x/2 })
          }
          maResult.toArray
        }
      }
      // Function: Return moving Standard deviation result, mixing "princeton.cs.introcs.StdStats"
      // Input:
      //   1. Array : focusData: inputsource
      //   2. Int   : window: section
      // output:
      //   1. Array : result of moving Standard deviation
      def movingStdDev(focusData: Array[Double] , window: Int) : Array[Double]  = {
        var stdDevArray = new ArrayBuffer[Double]
        var targetData1 = focusData.sliding(window).toArray

        var length = targetData1.size
        for(i <- 0 until length ){
          var resultDev = StdStats.stddev(targetData1(i))
          //println(resultDev,i)
          stdDevArray.append(resultDev)
        }
        stdDevArray.toArray
      }
      ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
      var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
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

      println(selloutDataFromOracle.show(5))
      println("oracle ok")
      //////////////////////////////////////////////////////////////////////////////////////////////////
      // 2. Load TempView & set the number of columns
      //////////////////////////////////////////////////////////////////////////////////////////////////
      var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol," +
        "a.regionid as accountid," +
        "a.product," +
        "a.yearweek," +
        "cast(a.qty as Double) as qty, " +
        "'test' as productname from keydata a" )
      // --------- Convert qty type: DecimalType => Double type ----------------
      //rawData.show(5)

      var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})
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
      // Clear out the abnormal value using the normal information
      var filterRdd = rawRdd.filter(x=> {
        // Data comes in line by line
        var checkValid = true
        // Assign yearweek information to variables
        var week = x.getString(yearweekNo).substring(4, 6).toInt
        // Assign abnormal to variables : Get rid of 53 week
        var standardWeek = 52
        if(week > standardWeek){
          checkValid = false
        }
        checkValid
      })
      /////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Processing        ////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // Distributed processing for each analytics key value (regionid, product)
      // 1. Sort Data
      // 2. Calculate moving average
      // 3. Calculate moving standard deviation
      // 4. Generate values for week (out of moving average)
      // 5. Merge all data-set for moving standard deviation
      // 6. Generate final-result
      // Row(regionid, product, yearweek, week, volume, movingValue, ratio, stddev, upper_bound, lower_bound , adjustedQty )
      var groupRddMap = filterRdd.
        groupBy(x=>{ (x.getString(keyNo), x.getString(accountidNo)) }).
        flatMap(x=>{
          var key = x._1
          var data = x._2
          // Debug code
          // var dataQtyNo = groupRddMapExpTest3.map(x=>{x._2.map(x=>{x.getDouble(qtyNo)})})

          // 1. Sort Data
          var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt})
          var sortedDataIndex = sortedData.zipWithIndex
          //debug code: data.first.map(x=>{x.getDouble(qtyNo)}).toArray
          var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNo))}).toArray
          var sortedSize = sortedData.map(x=>{ (x.getDouble(qtyNo))}).size
          var sortedVolumeIndex = sortedVolume.zipWithIndex

          var scope = 17
          var subScope = (scope.toDouble / 2.0).floor.toInt
          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // 2. Calculate moving average
          var movingResult = movingAverage(sortedVolume,scope)
          //3. Generate value for weeks (out of moving average)
          var preMAArray = new ArrayBuffer[Double]
          var postMAArray = new ArrayBuffer[Double]
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
            preMAArray.append(scopedAverage)

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
          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // 4. Merge all data-set for moving average
          var maResult = (preMAArray++movingResult++postMAArray.reverse).zipWithIndex
          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // 5. Calcalte moving standard deviations
          // 5._1 Convert maResult to hold the array type
          var maResultArr = (preMAArray++movingResult++postMAArray.reverse).toArray

          val range  = 5
          var halfRange = Math.floor(range /2 ).toInt

          var movingStddev = movingStdDev(maResultArr,range)

          var preMvStdDev = new ArrayBuffer[Double]()
          var postMvStdDev = new ArrayBuffer[Double]()

          for (i <- 0 until halfRange) {
            var frontStdDev =  StdStats.stddev(maResult.filter(x => x._2 >= 0 && x._2 <= ( i + halfRange)).
              map(x => { x._1  }).toArray)
            preMvStdDev.append(frontStdDev)
          }

          for (index <- 0 until halfRange){

            var backStdDev = StdStats.stddev(
              maResult.filter(x => { ((x._2 >= (lastIndex - halfRange - index)) &&
                (x._2 <= lastIndex))
              }).map(x => {x._1}).toArray
            )
            postMvStdDev.append(backStdDev)
          }
          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // 5._2 Merge all calculated data for moving standard deviations
          var mvStdDevArray  = (preMvStdDev++movingStddev++postMvStdDev.reverse).toArray
          var calMvResult = maResult.zip(mvStdDevArray)
          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // 6. Generate final-result
          var finalResult = sortedDataIndex.zip(calMvResult).map(x=>{
            var firstRow = x._1
            var zippedResult = x._2

            var key = firstRow._1.getString(keyNo)
            var regionid = key.split("_")(0)
            var product = key.split("_")(1)
            var yearweek = firstRow._1.getString(yearweekNo)
            var week = yearweek.substring(4,6)
            var volume = firstRow._1.getDouble(qtyNo)

            var movingValue = zippedResult._1._1

            var stddev = zippedResult._2
            var upper_bound = movingValue + stddev
            var lower_bound = movingValue - stddev

            if(upper_bound > 0.0d){
              upper_bound
            }else{
              if(lower_bound < 0.0d){
                lower_bound = 0.0d
              }
            }
            var adjustedQty = volume
            if(volume > upper_bound){
              volume = upper_bound
            }else{
              if(volume < lower_bound){
                volume = lower_bound
              }
              adjustedQty
            }
            Row(regionid, product, yearweek, week, volume, movingValue, stddev, upper_bound, lower_bound , adjustedQty )
          })
          finalResult

          var adjustedQty = finalResult.map(x=>{x.getDouble(9)}).toArray
          var smmothingArr = movingAverage(adjustedQty, 5).zipWithIndex

          var preSMTHrray = new ArrayBuffer[Double]
          var postSMTHrray = new ArrayBuffer[Double]

          var lastIndexSm = smmothingArr.size-1
          var ExtraSubScope = Math.floor(5/2).toInt

          for (index <- 0 until ExtraSubScope) {
            var scopedFirst = smmothingArr.
              filter(x => x._2 >= 0 && x._2 <= (index + ExtraSubScope)).
              map(x => {
                x._1
              })
            preSMTHrray.append(scopedFirst.sum / scopedFirst.size)

            var scopedLast = smmothingArr.
              filter(x => {
                ((x._2 >= (lastIndexSm - index - ExtraSubScope)) &&
                  (x._2 <= lastIndex))
              }).map(x => {
              x._1
            })
            postSMTHrray.append(scopedLast.sum / scopedLast.size)
          }

          var smoothing = preSMTHrray++smmothingArr.map(x=>{x._1})++postSMTHrray

          var finalResultZip = finalResult.zip(smoothing).map(f=>{
            // var debugZip = finalResutl1.zip(smoothing).map(f=>{f._1).head
            // var debungZipSmth = finalResutl1.zip(smoothing).head._2
            var regionid = f._1.getString(0)
            var product =  f._1.getString(1)
            var yearweek = f._1.getString(2)
            var week = f._1.getString(3)
            var volume = f._1.getDouble(4)
            var movingValue = f._1.getDouble(5)
            var stddev = f._1.getDouble(6)
            var upper_bound =  f._1.getDouble(7)
            var lower_bound = f._1.getDouble(8)
            var adjustedQty = f._1.getDouble(9)
            var smoothed = f._2

            Row(regionid, product, yearweek,week,
              volume,  movingValue, stddev, upper_bound, lower_bound
              , adjustedQty,smoothed )
          })
          finalResultZip
        })
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////


      // Convert voulume, qty, ratio type : DoubleType
      val middleResultTest = spark.createDataFrame(groupRddMap,
        StructType(Seq(StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("WEEK", StringType),
          StructField("VOLUME",DoubleType ),
          StructField("MA", DoubleType),
          StructField("MA_STDDEV", DoubleType),
          StructField("HIGH_BOUND", DoubleType),
          StructField("LOWER_BOUND", DoubleType),
          StructField("ADJUSTED_QTY", DoubleType),
          StructField("SMOOTHED", DoubleType)
        )))

      // Create temporay table
      middleResultTest.createOrReplaceTempView("intermidateTable")
      var finalResultDf = spark.sqlContext.sql("select REGIONID, PRODUCT, WEEK, VOLUME, MA, MA_STDDEV, ADJUSTED_QTY, SMOOTHED, " +
        " (VOLUME / SMOOTHED) AS STABLE_RATIO, (ADJUSTED_QTY / SMOOTHED) AS NON_STABLE_RATIO"  +
        " from intermidateTable")

      // Final result : grouby full data + Hive QL
      finalResultDf.createOrReplaceTempView("si_intermidate")
      var si_final = spark.sqlContext.sql("select regionid, product, week, avg(stable_ratio), avg(non_stable_ratio)" +
        " from si_intermidate group by regionid, product, week")


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
      var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
      val prop = new java.util.Properties
      prop.setProperty("driver", "oracle.jdbc.OracleDriver")
      //var staticUser = "kopo"
      //var staticPw = "kopo"
      prop.setProperty("user", staticUser)
      prop.setProperty("password", staticPw)
      //val beforeHive = "kopo_fullColumn_result"
      val seasonFinal = "kopo_si_update_result"

      middleResultTest.write.mode("overwrite").jdbc(outputUrl, seasonFinal, prop)
      si_final.write.mode("overwrite").jdbc(outputUrl, seasonFinal, prop)

      println("The seasonality model is transmitted in Oracle DB")
    }



  }
