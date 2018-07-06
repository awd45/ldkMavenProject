package 문제원형실습

object T2 {

    def main(args: Array[String]): Unit = {

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Library Definition ////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      // Define basic library
      import org.apache.spark.sql.Row

      // Define library Related to RDD Registry
      import org.apache.spark.sql.types.StructType
      import org.apache.spark.sql.types.StringType
      import org.apache.spark.sql.types.StructField
      import org.apache.spark.sql.types.DoubleType

      // Functions for week Calculation
      import java.util.Calendar
      import java.text.SimpleDateFormat
      import org.apache.spark.sql.SparkSession

      import edu.princeton.cs.introcs.StdStats

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
      } // end of function

      // Function: Return standard deviation result
      // Input:
      //   1. Array : targetData: inputsource
      //   2. Int   : myorder: section
      // output:
      //   1. Array : result of standard deviation
      def stdDev(targetData: Array[Double], myorder: Int): Array[Double] = {
        val length = targetData.size
        if (myorder > length || myorder <= 2) {
          throw new IllegalArgumentException
        } else {
          //var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)
          var stddevresult = targetData.sliding(myorder).map(StdStats.stddev(_))

          stddevresult.toArray
        }
      } // end of function

      // Purpose of Function : Calculate pre week
      def preWeek(inputYearWeek: String, gapWeek: Int): String = {
        val currYear = inputYearWeek.substring(0, 4).toInt
        val currWeek = inputYearWeek.substring(4, 6).toInt

        val calendar = Calendar.getInstance();
        calendar.setMinimalDaysInFirstWeek(4);
        calendar.setFirstDayOfWeek(Calendar.MONDAY);

        var dateFormat = new SimpleDateFormat("yyyyMMdd");

        calendar.setTime(dateFormat.parse(currYear + "1231"));
        //    calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        if (currWeek <= gapWeek) {
          var iterGap = gapWeek - currWeek
          var iterYear = currYear - 1

          calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
          var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

          while (iterGap > 0) {
            if (iterWeek <= iterGap) {
              iterGap = iterGap - iterWeek
              iterYear = iterYear - 1
              calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
              iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
            } else {
              iterWeek = iterWeek - iterGap
              iterGap = 0
            } // end of if
          } // end of while

          return iterYear.toString + "%02d".format(iterWeek)
        } else {
          var resultYear = currYear
          var resultWeek = currWeek - gapWeek

          return resultYear.toString + "%02d".format(resultWeek)
        } // end of if
      } // end of function

      // User Defined Function
      // Purpose of Funtion : Calculate post week
      def postWeek(inputYearWeek: String, gapWeek: Int): String = {
        val currYear = inputYearWeek.substring(0, 4).toInt
        val currWeek = inputYearWeek.substring(4, 6).toInt

        val calendar = Calendar.getInstance();
        calendar.setMinimalDaysInFirstWeek(4);
        calendar.setFirstDayOfWeek(Calendar.MONDAY);

        var dateFormat = new SimpleDateFormat("yyyyMMdd");

        calendar.setTime(dateFormat.parse(currYear + "1231"));

        var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        if (maxWeek < currWeek + gapWeek) {
          var iterGap = gapWeek + currWeek - maxWeek
          var iterYear = currYear + 1

          calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
          var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

          while (iterGap > 0) {
            if (iterWeek < iterGap) {
              iterGap = iterGap - iterWeek
              iterYear = iterYear + 1
              calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
              iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
            } else {
              iterWeek = iterGap
              iterGap = 0
            } // end of if
          } // end of while

          return iterYear.toString() + "%02d".format(iterWeek)
        } else {
          return currYear.toString() + "%02d".format((currWeek + gapWeek))
        } // end of if
      } // end of function

      ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
      var spark = SparkSession.builder().config("spark.master","local").getOrCreate()

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
      var staticUser = "kopo"
      var staticPw = "kopo"
      var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"

      val selloutDataFromOracle = spark.read.format("jdbc").
        options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

      selloutDataFromOracle.createOrReplaceTempView("keydata")

      println(selloutDataFromOracle.show())
      println("oracle ok")

      //////////////////////////////////////////////////////////////////////////////////////////////////
      // 2. data refining
      //////////////////////////////////////////////////////////////////////////////////////////////////


      var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
        "a.regionid as regionid, " +
        "a.product, " +
        "a.yearweek, " +
        "substr(a.yearweek,0,4) as year," +
        "substr(a.yearweek,5) as week," +
        "cast(a.qty as String) as qty, " +
        "cast(0.0 as Double) as ma, " +
        "cast(0.0 as Double) as stdev, " +
        "cast(0.0 as Double) as ref_qty, " +
        "cast(0.0 as Double) as smoothing " +
        " from keydata a" +
        " where substr(a.yearweek,5) != 53" )

      rawData.createOrReplaceTempView("rawData")

      rawData.show(2)

      var rawDataColumns = rawData.columns
      var keyNo = rawDataColumns.indexOf("keycol")
      var regionidNo = rawDataColumns.indexOf("regionid")
      var productNo = rawDataColumns.indexOf("product")
      var yearweekNo = rawDataColumns.indexOf("yearweek")
      var yearNo = rawDataColumns.indexOf("year")
      var weekNo = rawDataColumns.indexOf("week")
      var qtyNo = rawDataColumns.indexOf("qty")
      var maNo = rawDataColumns.indexOf("ma")
      var stdevNo = rawDataColumns.indexOf("stdev")
      var ref_qtyNo = rawDataColumns.indexOf("ref_qty")
      var smoothingNo = rawDataColumns.indexOf("smoothing")

      var rawRdd = rawData.rdd

      // QTY < 0 REFINE (rawRdd)
      var rawRdd1 = rawRdd.map(x => {
        var key = x.getString(keyNo)
        var regionid = x.getString(regionidNo)
        var product = x.getString(productNo)
        var yearweek = x.getString(yearweekNo)
        var qty = x.getString(qtyNo).toDouble
        var ma = x.getDouble(maNo)
        var std = x.getDouble(stdevNo)
        var ref_qty = x.getDouble(ref_qtyNo)
        var year = yearweek.substring(0,4)
        var week = yearweek.substring(4,6)
        var smoothing = x.getDouble(smoothingNo)
        if (qty < 0) {
          qty = 0
        }
        Row(
          key,
          regionid,
          product,
          yearweek,
          year,
          week,
          qty.toString,
          ma,
          std,
          ref_qty,
          smoothing)
      })

      var  addData = spark.sql("select concat(a.regionid,'_',a.product) as keycol, " +
        "a.regionid as regionid, " +
        "a.product, " +
        "a.yearweek, " +
        "substr(a.yearweek,0,4) as year," +
        "substr(a.yearweek,5) as week," +
        "cast(a.qty as String) as qty " +
        " from keydata a" +
        " where substr(a.yearweek,5) = 53")

      addData.createOrReplaceTempView("addData")


      var addDataColumns = addData.columns
      var keyNo = addDataColumns.indexOf("keycol")
      var regionidNo = addDataColumns.indexOf("regionid")
      var productNo = addDataColumns.indexOf("product")
      var yearweekNo = addDataColumns.indexOf("yearweek")
      var yearNo = addDataColumns.indexOf("year")
      var weekNo = addDataColumns.indexOf("week")
      var qtyNo = addDataColumns.indexOf("qty")

      var addRdd = addData.rdd

      // QTY < 0 REFINE (addRdd)
      var addRdd1 = addRdd.map(x => {
        var key = x.getString(keyNo)
        var regionid = x.getString(regionidNo)
        var product = x.getString(productNo)
        var yearweek = x.getString(yearweekNo)
        var year = yearweek.substring(0,4)
        var week = yearweek.substring(4,6)
        var qty = x.getString(qtyNo).toDouble
        if (qty < 0) {
          qty = 0
        }
        Row(
          key,
          regionid,
          product,
          yearweek,
          year,
          week,
          qty.toString)
      })



      // week53 collectAsMap
      var collecMaps2 = addRdd1.groupBy(x=>{
        ( x.getString(regionidNo), x.getString(productNo),x.getString(yearNo)
        )
      }).map(x=>{

        var key = x._1
        var data = x._2
        var value = (x._2.map(x=>{x.getString(qtyNo)}).head.toDouble/2).toInt

        (key,value)
      }).collectAsMap

      // add 53_qty to 52 & 01
      var analyticsMap = rawRdd1.map(row=> {

        var analyticsregionid = row.getString(regionidNo)
        var analyticsproduct = row.getString(productNo)
        var analyticsyearweek = row.getString(yearweekNo)
        var qty = row.getString(qtyNo).toDouble

        if (analyticsyearweek == "201552" || analyticsyearweek == "201601") {
          var plus = 0.0d
          if(collecMaps2.contains(analyticsregionid, analyticsproduct, "2015")){
            plus = collecMaps2(analyticsregionid, analyticsproduct, "2015")
          }
          qty = qty + plus
        }
        Row(
          row.getString(keyNo),
          row.getString(regionidNo),
          row.getString(productNo),
          row.getString(yearweekNo),
          row.getString(yearNo),
          row.getString(weekNo),
          qty,
          row.getDouble(maNo),
          row.getDouble(stdevNo),
          row.getDouble(ref_qtyNo),
          row.getDouble(smoothingNo))
      })

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Processing        ////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // (1) Moving Average Calc
      // Distributed processing for each analytics key value (regionid, product)
      // 1. Sort Data
      // 2. Calculate moving average
      // 3. Generate values for week (out of moving average)
      // 4. Merge all data-set for moving average
      // 5. Generate final-result
      var groupRddMapMA = analyticsMap.
        groupBy(x=>{ (x.getString(regionidNo), x.getString(productNo)) }).
        flatMap(x=>{

          var key = x._1
          var data = x._2

          // 1. Sort Data
          var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt})
          var sortedDataIndex = sortedData.zipWithIndex

          var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNo))}).toArray
          var sortedVolumeIndex = sortedVolume.zipWithIndex

          var scope = 13
          var subScope = (scope.toDouble / 2.0).floor.toInt

          // 2. Calculate moving average
          var movingResult = movingAverage(sortedVolume,scope)

          // 3. Generate value for weeks (out of moving average)
          var preMAArray = new ArrayBuffer[Double]
          var postMAArray = new ArrayBuffer[Double]

          var lastIndex = sortedVolumeIndex.size-1
          for (index <- 0 until subScope) {
            var scopedDataFirst = sortedVolumeIndex.
              filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
              map(x => {x._1})

            var scopedSum = scopedDataFirst.sum
            var scopedSize = scopedDataFirst.size
            var scopedAverage = scopedSum/scopedSize
            preMAArray.append(scopedAverage)

            var scopedDataLast = sortedVolumeIndex.
              filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
                (x._2 <= lastIndex)    ) }).
              map(x => {x._1})
            var secondScopedSum = scopedDataLast.sum
            var secondScopedSize = scopedDataLast.size
            var secondScopedAverage = secondScopedSum/secondScopedSize
            postMAArray.append(secondScopedAverage)
          }

          // 4. Merge all data-set for moving average
          var maResult = (preMAArray++movingResult++postMAArray.reverse).zipWithIndex

          // 5. Generate final-result
          var finalResult = sortedDataIndex.
            zip(maResult).
            map(x=>{
              var key = x._1._1.getString(keyNo)
              var regionid = x._1._1.getString(regionidNo)
              var product = x._1._1.getString(productNo)
              var yearweek = x._1._1.getString(yearweekNo)
              var qty = x._1._1.getDouble(qtyNo)
              var movingValue = x._1._1.getDouble(maNo)
              movingValue = x._2._1
              var year = yearweek.substring(0,4)
              var week = yearweek.substring(4,6)
              var stdev = x._1._1.getDouble(stdevNo)
              var ref_qty = x._1._1.getDouble(ref_qtyNo)
              var smoothing = x._1._1.getDouble(smoothingNo)
              Row(
                key,
                regionid,
                product,
                yearweek,
                year,
                week,
                qty,
                movingValue,
                stdev,
                ref_qty,
                smoothing)
            })
          finalResult
        })

      // stdev 추가
      // (2) Standard Deviation Calc.
      // Distributed processing for each analytics key value (regionid, product)
      // 1. Sort Data
      // 2. Calculate Standard Dev.
      // 3. Generate values for week (out of STDEV)
      // 4. Merge all data-set for STDEV
      // 5. Generate final-result
      var groupRddMapSTDEV = groupRddMapMA.
        groupBy(x=>{ (x.getString(regionidNo), x.getString(productNo)) }).
        flatMap(x=>{

          var key = x._1
          var data = x._2

          // 1. Sort Data
          var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt})
          var sortedDataIndex = sortedData.zipWithIndex

          var sortedVolume = sortedData.map(x=>{ (x.getDouble(maNo))}).toArray
          var sortedVolumeIndex = sortedVolume.zipWithIndex

          var scope = 5
          var subScope = (scope.toDouble / 2.0).floor.toInt

          // 2. Calculate moving average
          var stDevResult = stdDev(sortedVolume,scope)

          // 3. Generate value for weeks (out of moving average)
          var prestDevArray = new ArrayBuffer[Double]
          var poststDevArray = new ArrayBuffer[Double]

          var lastIndex = sortedVolumeIndex.size-1
          for (index <- 0 until subScope) {
            var scopedDataFirst = sortedVolumeIndex.
              filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
              map(x => {x._1})

            var scopedstDev = StdStats.stddev(scopedDataFirst)
            prestDevArray.append(scopedstDev)

            var scopedDataLast = sortedVolumeIndex.
              filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
                (x._2 <= lastIndex)    ) }).
              map(x => {x._1})
            var secondScopedstDev = StdStats.stddev(scopedDataLast)
            poststDevArray.append(secondScopedstDev)
          }

          // 4. Merge all data-set for moving average
          var STDEVResult = (prestDevArray++stDevResult++poststDevArray.reverse).zipWithIndex

          // 5. Generate final-result
          var finalResult = sortedDataIndex.
            zip(STDEVResult).
            map(x=>{
              var key = x._1._1.getString(keyNo)
              var regionid = x._1._1.getString(regionidNo)
              var product = x._1._1.getString(productNo)
              var yearweek = x._1._1.getString(yearweekNo)
              var qty = x._1._1.getDouble(qtyNo)
              var movingValue = x._1._1.getDouble(maNo)
              var stdevValue = x._1._1.getDouble(stdevNo)
              stdevValue = x._2._1
              var ref_qty = x._1._1.getDouble(ref_qtyNo)
              var year = yearweek.substring(0,4)
              var week = yearweek.substring(4,6)
              var smoothing = x._1._1.getDouble(smoothingNo)
              Row(
                key,
                regionid,
                product,
                yearweek,
                year,
                week,
                qty,
                movingValue,
                stdevValue,
                ref_qty,
                smoothing)
            })
          finalResult
        })

      // ref_qty 계산
      // (3) ref_qty Calc.
      // Distributed processing for each analytics key value (regionid, product)
      // 1. Sort Data
      // 2. Calculate upper / lower bound
      // 3. Calculate refined qty
      // 4. Generate final-result

      var groupRddMapREF_QTY = groupRddMapSTDEV.map(x => {
        var key = x.getString(keyNo)
        var regionid = x.getString(regionidNo)
        var product = x.getString(productNo)
        var yearweek = x.getString(yearweekNo)
        var qty = x.getDouble(qtyNo)
        var ma = x.getDouble(maNo)
        var std = x.getDouble(stdevNo)
        var ref_qty = x.getDouble(ref_qtyNo)
        var year = yearweek.substring(0,4)
        var week = yearweek.substring(4,6)
        var smoothing = x.getDouble(smoothingNo)
        var upperB = ma + std
        var lowerB = ma - std
        if (qty > upperB) {
          ref_qty = upperB
        } else if (qty < lowerB) {
          ref_qty = lowerB
        } else {
          ref_qty = qty
        }
        Row(
          key,
          regionid,
          product,
          yearweek,
          year,
          week,
          qty,
          ma,
          std,
          ref_qty,
          smoothing)
      })

      // smoothing 추가 / Ratio1,2 추가 (ratio 0->1)
      // (4) Smoothing Calc.
      // (5) Ratio Calc. for firm market & unfirm
      // Distributed processing for each analytics key value (regionid, product)
      // 1. Sort Data
      // 2. Calculate Smoothing
      // 3. Generate values for week (out of Smoothing)
      // 4. Merge all data-set for Smoothing
      // 5. Generate final-result
      var groupRddMapSMOOTHING = groupRddMapREF_QTY.
        groupBy(x=>{ (x.getString(regionidNo), x.getString(productNo)) }).
        flatMap(x=>{

          var key = x._1
          var data = x._2

          // 1. Sort Data
          var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt})
          var sortedDataIndex = sortedData.zipWithIndex

          var sortedVolume = sortedData.map(x=>{ (x.getDouble(ref_qtyNo))}).toArray
          var sortedVolumeIndex = sortedVolume.zipWithIndex

          var scope = 5
          var subScope = (scope.toDouble / 2.0).floor.toInt

          // 2. Calculate moving average
          var smthResult = movingAverage(sortedVolume,scope)

          // 3. Generate value for weeks (out of moving average)
          var presmoothingArray = new ArrayBuffer[Double]
          var postsmoothingArray = new ArrayBuffer[Double]

          var lastIndex = sortedVolumeIndex.size-1
          for (index <- 0 until subScope) {
            var scopedDataFirst = sortedVolumeIndex.
              filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
              map(x => {x._1})

            var scopedSum = scopedDataFirst.sum
            var scopedSize = scopedDataFirst.size
            var scopedAverage = scopedSum/scopedSize
            presmoothingArray.append(scopedAverage)

            var scopedDataLast = sortedVolumeIndex.
              filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
                (x._2 <= lastIndex)    ) }).
              map(x => {x._1})
            var secondScopedSum = scopedDataLast.sum
            var secondScopedSize = scopedDataLast.size
            var secondScopedAverage = secondScopedSum/secondScopedSize
            postsmoothingArray.append(secondScopedAverage)
          }

          // 4. Merge all data-set for moving average
          var smoothingResult = (presmoothingArray++smthResult++postsmoothingArray.reverse).zipWithIndex

          // 5. Generate final-result
          var finalResult = sortedDataIndex.
            zip(smoothingResult).
            map(x=>{
              var key = x._1._1.getString(keyNo)
              var regionid = x._1._1.getString(regionidNo)
              var product = x._1._1.getString(productNo)
              var yearweek = x._1._1.getString(yearweekNo)
              var qty = x._1._1.getDouble(qtyNo)
              var movingValue = x._1._1.getDouble(maNo)
              var stdevValue = x._1._1.getDouble(stdevNo)
              var year = yearweek.substring(0,4)
              var week = yearweek.substring(4,6)
              var ref_qty = x._1._1.getDouble(ref_qtyNo)
              var smoothing = x._1._1.getDouble(smoothingNo)
              smoothing = x._2._1
              var ratio1 = 1.0d
              var ratio2 = 1.0d
              if(smoothing != 0.0d){
                ratio1 = qty / smoothing
                ratio2 = ref_qty / smoothing
              }
              if(ratio1 == 0){
                ratio1 = 1.0
              }
              if(ratio2 == 0){
                ratio2 = 1.0
              }
              Row(
                key,
                regionid,
                product,
                yearweek,
                year,
                week,
                qty.toString,
                movingValue.toString,
                stdevValue.toString,
                ref_qty.toString,
                smoothing.toString,
                ratio1.toString,
                ratio2.toString)
            })
          finalResult
        })



      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      val middleResult = spark.createDataFrame(groupRddMapSMOOTHING,
        StructType(Seq(StructField("KEYID", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("YEAR", StringType),
          StructField("WEEK", StringType),
          StructField("QTY", StringType),
          StructField("MA", StringType),
          StructField("STDEV", StringType),
          StructField("REF_QTY", StringType),
          StructField("SMOOTHING", StringType),
          StructField("RATIO_FIRM", StringType),
          StructField("RATIO_UNFIRM", StringType))))

      middleResult.createOrReplaceTempView("middleTable")


      var finalResultDf = spark.sql("select REGIONID, PRODUCT, WEEK, AVG(QTY) AS AVG_QTY, AVG(MA) AS AVG_MA," +
        " AVG(STDEV) AS AVG_STDEV , AVG(REF_QTY) AS AVG_REF_QTY, AVG(SMOOTHING) AS AVG_SMOOTHING, " +
        " AVG(RATIO_FIRM) AS AVG_RATIO_FIRM, AVG(RATIO_UNFIRM) AS AVG_RATIO_UNFIRM" +
        " from middleTable group by REGIONID, PRODUCT, WEEK")


      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Uploading        ////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // File type
      finalResultDf.
        coalesce(1).
        write.format("csv").
        mode("overwrite").
        option("charset","ISO-8859-1").
        option("header","true").
        save("season_result")QN


      // Database type
      val prop = new java.util.Properties
      prop.setProperty("driver", "oracle.jdbc.OracleDriver")
      prop.setProperty("user", "kopo")
      prop.setProperty("password", "kopo")
      val table = "GROUP2_RESULT"

      staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

      finalResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)
      println("spark test completed")

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////// middle to DB to chk full yearweek ratio1, ratio2
      middleResult.
        coalesce(1).
        write.format("csv").
        mode("overwrite").
        option("charset","ISO-8859-1").
        option("header","true").
        save("season_middle_result")

      ///////////////////////////////////////////////////////////////////////////////////
      //// moving_average to db /////////////////////////////////////////////////////////
      ///////////////////////////////////////////////////////////////////////////////////
      val MAResult = spark.createDataFrame(groupRddMapMA,
        StructType(Seq(StructField("KEYID", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("YEAR", StringType),
          StructField("WEEK", StringType),
          StructField("QTY", DoubleType),
          StructField("MA", DoubleType),
          StructField("STDEV", DoubleType),
          StructField("REF_QTY", DoubleType),
          StructField("SMOOTHING", DoubleType))))

      MAResult.createOrReplaceTempView("matable")


      var MAResultDf = spark.sql("select REGIONID, PRODUCT, WEEK, AVG(QTY) AS AVG_QTY, AVG(MA) AS AVG_MA " +
        " from matable group by REGIONID, PRODUCT, WEEK")

      // File type
      MAResultDf.
        coalesce(1).
        write.format("csv").
        mode("overwrite").
        option("charset","ISO-8859-1").
        option("header","true").
        save("season_ma_result")


      // Database type
      var table = "GROUP2_MA_RESULT"
      MAResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)

      ///////////////////////////////////////////////////////////////////////////////////
      //// standard deviation to db /////////////////////////////////////////////////////
      ///////////////////////////////////////////////////////////////////////////////////


      val STDEVResult = spark.createDataFrame(groupRddMapSTDEV,
        StructType(Seq(StructField("KEYID", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("YEAR", StringType),
          StructField("WEEK", StringType),
          StructField("QTY", DoubleType),
          StructField("MA", DoubleType),
          StructField("STDEV", DoubleType),
          StructField("REF_QTY", DoubleType),
          StructField("SMOOTHING", DoubleType))))

      STDEVResult.createOrReplaceTempView("stdevtable")


      var STDEVResultDf = spark.sql("select REGIONID, PRODUCT, WEEK, AVG(QTY) AS AVG_QTY, AVG(MA) AS AVG_MA," +
        " AVG(STDEV) AS AVG_STDEV" +
        " from stdevtable group by REGIONID, PRODUCT, WEEK")

      // File type
      STDEVResultDf.
        coalesce(1).
        write.format("csv").
        mode("overwrite").
        option("charset","ISO-8859-1").
        option("header","true").
        save("season_stdev_result")


      // Database type
      var table = "GROUP2_STDEV_RESULT"
      STDEVResultDf.write.mode("overwrite").jdbc(staticUrl, table, prop)


    }
  }

