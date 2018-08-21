//package 문제원형실습
//
//object T1 {
//
//  import breeze.numerics.round
//
//  object project_parameter {
//    def main(args: Array[String]): Unit = {
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Library Definition ////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      import edu.princeton.cs.introcs.StdStats
//      import org.apache.spark.sql.{Row, SparkSession}
//      import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
//
//      import scala.collection.mutable.ArrayBuffer
//
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Function Definition ////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//
//      //   1. Array : 이동평균
//      def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
//        val length = targetData.size
//        if (myorder > length || myorder <= 2) {
//          throw new IllegalArgumentException
//        } else {
//          var maResult = targetData.sliding(myorder).map(x=>{x.sum}).map(x=>{ x/myorder })
//
//          if (myorder % 2 == 0) { //짝수일 경우는 쓰지않지만 예외처리로서 사용
//            maResult = maResult.sliding(2).map(x=>{x.sum}).map(x=>{ x/2 })
//          }
//          maResult.toArray
//        }
//      }
//
//      // 2. 변동률 계산(표준편차)
//      def stdevDf(targetData: Array[Double], myorder: Int): Array[Double] = {
//        val length = targetData.size
//        if (myorder > length || myorder <= 2) {
//          throw new IllegalArgumentException
//        } else {
//          var maResult1 = targetData.sliding(myorder).toArray
//          var stddev1 = new ArrayBuffer[Double]
//
//          for (i <- 0 until maResult1.length){
//            var maResult2 = maResult1(i)
//            var stddev = StdStats.stddev(maResult2)
//            stddev1.append(stddev)
//          }
//          if (myorder % 2 == 0) { //짝수일 경우는 쓰지않지만 예외처리로서 사용
//            for (i <- 0 until maResult1.length){
//              var maResult2 = maResult1(i)
//              var stddev = StdStats.stddev(maResult2)
//              stddev1.append(stddev)
//            }
//          }
//          stddev1.toArray
//        }
//
//      }
//
//
//      ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
//      var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
//
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Data Loading   ////////////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      // Path setting
//      //    var dataPath = "./data/"
//      //    var mainFile = "kopo_channel_seasonality_ex.csv"
//      //    var subFile = "kopo_product_master.csv"
//      //
//      //    var path = "c:/spark/bin/data/"
//      //
//      //    // Absolute Path
//      //    //kopo_channel_seasonality_input
//      //    var mainData = spark.read.format("csv").option("header", "true").load(path + mainFile)
//      //    var subData = spark.read.format("csv").option("header", "true").load(path + subFile)
//      //
//      //    spark.catalog.dropTempView("maindata")
//      //    spark.catalog.dropTempView("subdata")
//      //    mainData.createTempView("maindata")
//      //    subData.createOrReplaceTempView("subdata")
//      //
//      //    /////////////////////////////////////////////////////////////////////////////////////////////////////
//      //    ////////////////////////////////////  Data Refining using sql////////////////////////////////////////
//      //    /////////////////////////////////////////////////////////////////////////////////////////////////////
//      //    var joinData = spark.sql("select a.regionid as accountid," +
//      //      "a.product as product, a.yearweek, a.qty, b.productname " +
//      //      "from maindata a left outer join subdata b " +
//      //      "on a.productgroup = b.productid")
//      //
//      //    joinData.createOrReplaceTempView("keydata")
//      // 1. data loading
//      //////////////////////////////////////////////////////////////////////////////////////////////////
//      var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
//      //staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
//      //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
//      var staticUser = "kopo"
//      var staticPw = "kopo"
//      var selloutDb = "kopo_channel_seasonality_new"
//
//      val selloutDataFromOracle = spark.read.format("jdbc").
//        options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
//
//      selloutDataFromOracle.createOrReplaceTempView("keydata")
//
//      println(selloutDataFromOracle.show())
//      println("oracle ok")
//
//      //////////////////////////////////////////////////////////////////////////////////////////////////
//      // 2. data refining
//      //////////////////////////////////////////////////////////////////////////////////////////////////
//
//      //(1)음수(반품)는 0으로 고정
//      var rawData = spark.sql("select regionid as regionid," +
//        "a.product as product," +
//        "a.yearweek as yearweek," +
//        "cast(a.qty as String) as qty, "+
//        "cast(case When qty <= 0 then 1 else qty end as String) as qty_new " +
//        "from keydata a" )
//
//      rawData.show(2)
//
//      //(2)인덱스설정
//      var rawDataColumns = rawData.columns.map(x=>{x.toLowerCase()})
//      var regionidNo = rawDataColumns.indexOf("regionid")
//      var productNo = rawDataColumns.indexOf("product")
//      var yearweekNo = rawDataColumns.indexOf("yearweek")
//      var qtyNo = rawDataColumns.indexOf("qty")
//      var qtyNewNo = rawDataColumns.indexOf("qty_new")
//
//      var rawRdd = rawData.rdd
//
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ///////////////////////////////////////파라미터//////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      var paramDb = "kopo_parameter_omz"
//
//      val paramDataFromOracle = spark.read.format("jdbc").
//        options(Map("url" -> staticUrl, "dbtable" -> paramDb, "user" -> staticUser, "password" -> staticPw)).load
//
//      paramDataFromOracle.createOrReplaceTempView("paramdata")
//      val parametersDF = spark.sql("select * from paramdata where use_yn = 'Y'")
//
//      var paramDataColumns = parametersDF.columns.map(x => {
//        x.toLowerCase()
//      })
//      var pCategoryNo = paramDataColumns.indexOf("param_category")
//      var pNameNo = paramDataColumns.indexOf("param_name")
//      var pValueNo = paramDataColumns.indexOf("param_value")
//
//      val parameterMap =
//        parametersDF.rdd.groupBy {x => (x.getString(pCategoryNo), x.getString(pNameNo))}.map(row => {
//          var paramValue = row._2.map(x=>{x.getString(pValueNo)}).toArray
//          ( (row._1._1, row._1._2), (paramValue) )
//        }).collectAsMap
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////// 정제된 데이터 필터시 필요한 파라미터//////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      var PRODUCTSET = selloutDataFromOracle.rdd.map(x => { x.getString(productNo) }).distinct().collect.toSet
//      if ( parameterMap.contains("COMMON","VALID_PRODUCT") ) {
//        PRODUCTSET = parameterMap("COMMON","VALID_PRODUCT").toSet
//      }
//      var REGIONIDSET = selloutDataFromOracle.rdd.map(x => { x.getString(regionidNo) }).distinct().collect.toSet
//      if ( parameterMap.contains("COMMON","VALID_REGIONID") ) {
//        REGIONIDSET = parameterMap("COMMON","VALID_REGIONID").toSet
//      }
//      var STARTYEAR = 2014
//      if ( parameterMap.contains("COMMON","VALID_START_YEAR") ) {
//        STARTYEAR = parameterMap("COMMON","VALID_START_YEAR")(0).toInt
//      }
//      var ENDYEAR = 2016
//      if ( parameterMap.contains("COMMON","VALID_END_YEAR") ) {
//        ENDYEAR = parameterMap("COMMON","VALID_END_YEAR")(0).toInt
//      }
//      var VALIDWEEK = 53
//      if ( parameterMap.contains("COMMON","VALID_WEEK") ) {
//        VALIDWEEK = parameterMap("COMMON","VALID_WEEK")(0).toInt
//      }
//
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ///////////////////////데이터 정제시 필요한 파라미터/////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      var RESTARTYEAR = 2014
//      if ( parameterMap.contains("COMMON","MA_START_YEAR") ) {
//        RESTARTYEAR = parameterMap("COMMON","MA_START_YEAR")(0).toInt
//      }
//      var REENDYEAR = 2016
//      if ( parameterMap.contains("COMMON","MA_END_YEAR") ) {
//        REENDYEAR = parameterMap("COMMON","MA_END_YEAR")(0).toInt
//      }
//      var REFINEDMA = 17
//      if(parameterMap.contains("COMMON", "MA_SECTION1")){
//        REFINEDMA = parameterMap("COMMON", "MA_SECTION1")(0).toInt
//      }
//      var REFINEDSTD = 9
//      if(parameterMap.contains("COMMON", "MA_SECTION2")){
//        REFINEDSTD = parameterMap("COMMON", "MA_SECTION2")(0).toInt
//      }
//
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////데이터 세이브 파라미터///////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      var SAVETABLE = "KOPO_CHANNEL_SEASONALITY_RESULT_OMZ"
//      if(parameterMap.contains("COMMON", "SAVE_TABLE_NAME")){
//        SAVETABLE = parameterMap("COMMON", "SAVE_TABLE_NAME")(0).toString
//      }
//
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////year, week 함수//////////////////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      def getWeekInfo(inputData: String): Int = {
//
//        var answer = inputData.substring(4, 6).toInt
//        answer
//      }
//
//      def getYearInfo(inputData: String): Int ={
//        var answer = inputData.substring(0, 4).toInt
//        answer
//      }
//      /////////////////////////////////////////////////////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//
//
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Data Filtering         ////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      // The abnormal value is refined using the normal information
//
//      //(3) 52주차가 넘어가면 제거, 52주차까지만 필터링
//      var filterEx1Rdd = rawRdd.filter(x=> {
//        // Data comes in line by line
//        var checkValid = false
//        // Assign yearweek information to variables
//        // Assign abnormal to variables
//        var standardWeek = 53
//        var weekInfo = getWeekInfo(x.getString(yearweekNo))
//        var yearInfo = getYearInfo(x.getString(yearweekNo))
//
//        if((weekInfo < standardWeek)&&
//          (RESTARTYEAR<=yearInfo && REENDYEAR>=yearInfo)){
//          checkValid = true
//        }
//        checkValid
//      })
//      var mapRdd = filterEx1Rdd.map(x=>{
//        var qty = x.getString(qtyNo).toDouble
//        var qty_new = x.getString(qtyNewNo).toDouble
//
//        Row( x.getString(regionidNo),
//          x.getString(productNo),
//          x.getString(yearweekNo),
//          qty, //x.getString(qtyNo),
//          qty_new)
//      })
//
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Data Processing        ////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      // Distributed processing for each analytics key value (regionid, product)
//      // 1. Sort Data
//      // 2. Calculate moving average
//      // 3. Generate values for week (out of moving average)
//      // 4. Merge all data-set for moving average
//      // 5. Generate final-result
//      // (key, account, product, yearweek, qty, productname)
//      var groupRddMapExp1 = mapRdd.
//        groupBy(x=>{ (x.getString(regionidNo), x.getString(productNo)) }).
//        flatMap(x=>{
//
//          var key = x._1
//          var data = x._2
//
//          // 1. Sort Data
//          var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekNo).toInt}) //연주차별로 순서대로 order by해주는곳
//          var sortedDataIndex = sortedData.zipWithIndex
//
//          var sortedVolume = sortedData.map(x=>{ (x.getDouble(qtyNewNo))}).toArray
//          var sortedVolumeIndex = sortedVolume.zipWithIndex
//
//          var scope = REFINEDMA
//          var scope1 = REFINEDSTD
//          var subScope = (scope.toDouble / 2.0).floor.toInt
//          var subScope1 = (scope1.toDouble / 2.0).floor.toInt
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          ///////////////////////////////////2. 이동평균(MA)///////////////////////////////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var movingResult = movingAverage(sortedVolume,scope)
//
//          ///////////////////////////////////2-1.이동평균의 빈값 채우기///////////////////////////
//          var preMAArray = new ArrayBuffer[Double]
//          var postMAArray = new ArrayBuffer[Double]
//
//          var lastIndex = sortedVolumeIndex.size-1
//
//          for (index <- 0 until subScope) {
//            var scopedDataFirst = sortedVolumeIndex.
//              filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
//              map(x => {x._1})
//
//            var scopedSum = scopedDataFirst.sum
//            var scopedSize = scopedDataFirst.size
//            var scopedAverage = scopedSum/scopedSize
//            preMAArray.append(scopedAverage)
//
//            var scopedDataLast = sortedVolumeIndex.
//              filter(x => { (  (x._2 >= (lastIndex - subScope - index)) &&
//                (x._2 <= lastIndex)    ) }).
//              map(x => {x._1})
//            var secondScopedSum = scopedDataLast.sum
//            var secondScopedSize = scopedDataLast.size
//            var secondScopedAverage = secondScopedSum/secondScopedSize
//            postMAArray.append(secondScopedAverage)
//          }
//          var maResult = (preMAArray++movingResult++postMAArray.reverse).toArray
//          var maResultIndex = maResult.zipWithIndex
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          //////////////////////////////////3.이동표준편차(MSTD)///////////////////////////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var mstd = stdevDf(maResult,scope1)
//          ///////////////////////////////////3-1.이동표준편차의 빈값 채우기///////////////////////
//          var preMAArray1 = new ArrayBuffer[Double]
//          var postMAArray1 = new ArrayBuffer[Double]
//          for (index <- 0 until subScope1) {
//            var maResultFirst = maResultIndex.
//              filter(x => x._2 >= 0 && x._2 <= (index+subScope1)).
//              map(x => {x._1})
//
//            var prestddev = StdStats.stddev(maResultFirst)
//            preMAArray1.append(prestddev)
//
//            var maResultLast = maResultIndex.
//              filter(x => { (  (x._2 >= (lastIndex - subScope1 - index)) &&
//                (x._2 <= lastIndex)    ) }).
//              map(x => {x._1})
//            var poststddev = StdStats.stddev(maResultLast)
//            postMAArray1.append(poststddev)
//          }
//          var MSTD = (preMAArray1++mstd++postMAArray1.reverse).toArray
//          var MSTDIndex = MSTD.zipWithIndex
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          //////////////////////////////////4. 상한(UPPER_BOUND1),하한(LOWER_BOUND1)///////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var UPPER_BOUND1 = new ArrayBuffer[Double]
//          var LOWER_BOUND1 = new ArrayBuffer[Double]
//
//          for (index <- 0 until maResult.size){
//            var UPPER_BOUND = (maResult(index)) + (MSTD(index))
//            UPPER_BOUND1.append(UPPER_BOUND)
//
//            var LOWER_BOUND = (maResult(index)) - (MSTD(index))
//            LOWER_BOUND1.append(LOWER_BOUND)
//          }
//
//          var UPPER_BOUND1Index = UPPER_BOUND1.zipWithIndex
//          var LOWER_BOUND1Index = LOWER_BOUND1.zipWithIndex
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          ///////////////////////////////////5. 정제된판매량(REFINED_QTY)//////////////////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var REFINED_QTY = new ArrayBuffer[Double]
//          var sales = 1.0d
//
//          for(index <- 0 until sortedVolume.size){
//            if(sortedVolume(index) > UPPER_BOUND1(index)){
//              sales = maResult(index)
//              REFINED_QTY.append(sales)
//            }else if(sortedVolume(index) < LOWER_BOUND1(index)){
//              sales = maResult(index)
//              REFINED_QTY.append(sales)
//            }else{
//              sales = sortedVolume(index)
//              REFINED_QTY.append(sales)
//            }
//          }
//          var StaSales = REFINED_QTY.toArray
//          var REFINED_QTYsIndex = (StaSales).zipWithIndex
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          //////////////////////////////////6.스무딩처리(SMOOTH)///////////////////////////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var movigStaSales = movingAverage(StaSales,scope1)
//          var preStaArray = new ArrayBuffer[Double]
//          var postStaArray = new ArrayBuffer[Double]
//          for (index <- 0 until subScope1) {
//            var staDataFirst = REFINED_QTYsIndex.
//              filter(x => x._2 >= 0 && x._2 <= (index + subScope1)).
//              map(x => {x._1})
//
//            var stadSum = staDataFirst.sum
//            var staSize = staDataFirst.size
//            var staAverage = stadSum/staSize
//            preStaArray.append(staAverage)
//
//            var scopedDataLast = REFINED_QTYsIndex.
//              filter(x => { (  (x._2 >= (lastIndex - subScope1 - index)) &&
//                (x._2 <= lastIndex)    ) }).
//              map(x => {x._1})
//            var secondstadSum = scopedDataLast.sum
//            var secondstaSize = scopedDataLast.size
//            var secondStaAverage = secondstadSum/secondstaSize
//            postStaArray.append(secondStaAverage)
//          }
//          var StaArray = (preStaArray++movigStaSales++postStaArray.reverse).toArray
//          var StaArrayIndex = StaArray.zipWithIndex
//
//          /////////////////////////////////////////////////////////////////////////////////////////
//          //////////////////////////////////7.final-result/////////////////////////////////////////
//          /////////////////////////////////////////////////////////////////////////////////////////
//          var finalResult = sortedDataIndex.
//            zip(maResultIndex).
//            map(x=>{
//
//              var REGIONID = x._1._1.getString(regionidNo)
//              var PRODUCT = x._1._1.getString(productNo)
//              var YEARWEEK = x._1._1.getString(yearweekNo)
//              var QTY = x._1._1.getDouble(4)
//              var QTY_NEW = x._1._1.getDouble(qtyNewNo)
//              var MA = x._2._1
//              var WEEK = YEARWEEK.substring(4,6)
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA)
//            })
//
//          var finalResult2 = finalResult.
//            zip(MSTDIndex).
//            map(x=>{
//              var REGIONID = x._1.getString(regionidNo)
//              var PRODUCT = x._1.getString(productNo)
//              var YEARWEEK = x._1.getString(yearweekNo)
//              var WEEK = YEARWEEK.substring(4,6)
//              var QTY = x._1.getDouble(4)
//              var QTY_NEW = x._1.getDouble(qtyNewNo)
//              var MA = x._1.getDouble(6)
//              var MSTD = x._2._1
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA,
//                MSTD)
//            })
//
//          var finalResult3 = finalResult2.
//            zip(UPPER_BOUND1Index).
//            map(x=>{
//              var REGIONID = x._1.getString(regionidNo)
//              var PRODUCT = x._1.getString(productNo)
//              var YEARWEEK = x._1.getString(yearweekNo)
//              var WEEK = YEARWEEK.substring(4,6)
//              var QTY = x._1.getDouble(4)
//              var QTY_NEW = x._1.getDouble(qtyNewNo)
//              var MA = x._1.getDouble(6)
//              var MSTD = x._1.getDouble(7)
//              var UPPER_BOUND = x._2._1
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA,
//                MSTD,
//                UPPER_BOUND)
//            })
//
//          var finalResult4 = finalResult3.
//            zip(LOWER_BOUND1Index).
//            map(x=>{
//              var REGIONID = x._1.getString(regionidNo)
//              var PRODUCT = x._1.getString(productNo)
//              var YEARWEEK = x._1.getString(yearweekNo)
//              var WEEK = YEARWEEK.substring(4,6)
//              var QTY = x._1.getDouble(4)
//              var QTY_NEW = x._1.getDouble(qtyNewNo)
//              var MA = x._1.getDouble(6)
//              var MSTD = x._1.getDouble(7)
//              var UPPER_BOUND = x._1.getDouble(8)
//              var LOWER_BOUND = x._2._1
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA,
//                MSTD,
//                UPPER_BOUND,
//                LOWER_BOUND)
//            })
//
//          var finalResult5 = finalResult4.
//            zip(REFINED_QTYsIndex).
//            map(x=>{
//              var REGIONID = x._1.getString(regionidNo)
//              var PRODUCT = x._1.getString(productNo)
//              var YEARWEEK = x._1.getString(yearweekNo)
//              var WEEK = YEARWEEK.substring(4,6)
//              var QTY = x._1.getDouble(4)
//              var QTY_NEW = x._1.getDouble(qtyNewNo)
//              var MA = x._1.getDouble(6)
//              var MSTD = x._1.getDouble(7)
//              var UPPER_BOUND = x._1.getDouble(8)
//              var LOWER_BOUND = x._1.getDouble(9)
//              var REFINED_QTY = x._2._1
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA,
//                MSTD,
//                UPPER_BOUND,
//                LOWER_BOUND,
//                REFINED_QTY)
//            })
//
//          //StaArrayIndex
//          var finalResult6 = finalResult5.
//            zip(StaArrayIndex).
//            map(x=>{
//              var REGIONID = x._1.getString(regionidNo)
//              var PRODUCT = x._1.getString(productNo)
//              var YEARWEEK = x._1.getString(yearweekNo)
//              var YEAR = YEARWEEK.substring(0,4)
//              var WEEK = YEARWEEK.substring(4,6)
//              var QTY = x._1.getDouble(4)
//              var QTY_NEW = x._1.getDouble(qtyNewNo)
//              var MA = x._1.getDouble(6)
//              var MSTD = x._1.getDouble(7)
//              var UPPER_BOUND = x._1.getDouble(8)
//              var LOWER_BOUND = x._1.getDouble(9)
//              var REFINED_QTY = x._1.getDouble(10)
//              var SMOOTH = x._2._1
//              var STABLE = 1.0d
//              var UNSTABLE = 1.0d
//              if(SMOOTH != 0){
//                STABLE = QTY_NEW/SMOOTH
//                UNSTABLE = REFINED_QTY/SMOOTH
//              }
//              Row(REGIONID.toString,
//                PRODUCT.toString,
//                YEARWEEK.toString,
//                YEAR,
//                WEEK,
//                QTY,
//                QTY_NEW,
//                MA,
//                MSTD,
//                UPPER_BOUND,
//                LOWER_BOUND,
//                REFINED_QTY,
//                SMOOTH,
//                STABLE,
//                UNSTABLE)
//            })
//          finalResult6
//          ///////////////////////////////////////////컬럼 검색/////////////////////////////////////////////////////
//          ////////QTY = 실제판매량, QTY_NEW = 실제판매량0처리, MA = 이동평균(판매추세량)                  /////////
//          ///////MSTD = 이동표준편차, UPPER_BOUND =상한, LOWER_BOUND = 하한, REFINED_QTY = 정제된 판매량/////////
//          ////// SMOOTH = 스무딩처리 값, STABLE = 안정된시장, UNSTABLE: 불안정시장                      ////////
//          ////////////////////////////////////////////////////////////////////////////////////////////////
//        })
//
//      /////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////고객이 원하는 데이터 값 찾는 필터 부분///////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////
//      var filterRdd1 = groupRddMapExp1.filter(x => {
//        var checkValid = false
//
//        var weekInfo = getWeekInfo(x.getString(yearweekNo))
//        var yearInfo = getYearInfo(x.getString(yearweekNo))
//
//        if ((weekInfo < VALIDWEEK) &&
//          ((STARTYEAR<=yearInfo && ENDYEAR>=yearInfo)) &&
//          (PRODUCTSET.contains(x.getString(productNo)))&&
//          (REGIONIDSET.contains(x.getString(regionidNo)))) {
//          checkValid = true
//        }
//        checkValid
//      })
//
//      ///////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ///////////////////////////////////////////컬럼/////////////////////////////////////////////////////
//      ////////QTY = 실제판매량, QTY_NEW = 실제판매량0처리 ,MA = 이동평균(판매추세량)                  /////////
//      ///////MSTD = 이동표준편차, UPPER_BOUND =상한, LOWER_BOUND = 하한, REFINED_QTY = 정제된 판매량/////////
//      ////// SMOOTH = 스무딩처리 값, STABLE = 안정된시장, UNSTABLE: 불안정시장                      ////////
//      ////////////////////////////////////////////////////////////////////////////////////////////////
//      val middleResult1 = spark.createDataFrame(filterRdd1,
//        StructType(Seq(StructField("REGIONID", StringType),
//          StructField("PRODUCT", StringType),
//          StructField("YEARWEEK", StringType),
//          StructField("YEAR", StringType),
//          StructField("WEEK", StringType),
//          StructField("QTY", DoubleType),
//          StructField("QTY_NEW", DoubleType),
//          StructField("MA", DoubleType),
//          StructField("MSTD", DoubleType),
//          StructField("UPPER_BOUND", DoubleType),
//          StructField("LOWER_BOUND", DoubleType),
//          StructField("REFINED_QTY", DoubleType),
//          StructField("SMOOTH", DoubleType),
//          StructField("STABLE", DoubleType),
//          StructField("UNSTABLE", DoubleType)
//        )))
//
//      middleResult1.createOrReplaceTempView("middleTable")
//
//      var finalResultDf = spark.sqlContext.sql("select REGIONID, PRODUCT, YEARWEEK, YEAR, WEEK, QTY, QTY_NEW, MA, MSTD, UPPER_BOUND,"+
//        "LOWER_BOUND, REFINED_QTY, SMOOTH, STABLE, UNSTABLE from middleTable")
//
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      ////////////////////////////////////  Data Unloading        ////////////////////////////////////////
//      /////////////////////////////////////////////////////////////////////////////////////////////////////
//      // File type
//      //    finalResultDf.
//      //      coalesce(1). // 파일개수
//      //      write.format("csv").  // 저장포맷
//      //      mode("overwrite"). // 저장모드 append/overwrite
//      //      save("season_result") // 저장파일명
//      //    println("spark test completed")
//      // Database type
//
//      ///DATA SAVE
//
//      var outUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
//      var outUser = "kopo"
//      var outcPw = "kopo"
//
//      val prop = new java.util.Properties
//      prop.setProperty("driver", "org.postgresql.Driver")
//      prop.setProperty("user", outUser)
//      prop.setProperty("password", outcPw)
//      val table = SAVETABLE
//
//      //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
//
//      finalResultDf.write.mode("overwrite").jdbc(outUrl, table, prop)
//      println("Seasonality model completed, Data Inserted in Oracle DB")
//    }
//
//  }
//
