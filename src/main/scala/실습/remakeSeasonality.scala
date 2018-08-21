package 실습

object remakeSeasonality {

  def main(args: Array[String]): Unit = {

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    import edu.princeton.cs.introcs.StdStats
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.sql.{Row, SparkSession}
    import scala.collection.mutable.ArrayBuffer

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    def dataLoad(sqlSelect: String, id: String, password: String, tableName: String, ipAsPortAsserviceName: String, tempName: String
                ): org.apache.spark.sql.DataFrame = {
      var staticUrl = ""
      var staticUser = id
      var staticPw = password
      var selloutDb = tableName
      if (sqlSelect == "oracle") {
        staticUrl = "jdbc:oracle:thin:@" + ipAsPortAsserviceName
      } else {
        staticUrl = "jdbc:" + sqlSelect + "://" + ipAsPortAsserviceName
      }

      var selloutData = spark.read.format("jdbc").
        options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

      selloutData.createOrReplaceTempView(tempName)

      println(selloutData.show())
      selloutData

    }

    def dataOut(sqlSelect: String, id: String, password: String, outTableName: String, ipAsPortAsserviceName: String, dfName: org.apache.spark.sql.DataFrame) = {
      var outputUrl = ""
      var outputUser = id
      var outputPw = password
      val prop = new java.util.Properties
      if (sqlSelect == "oracle") {
        outputUrl = "jdbc:oracle:thin:@" + ipAsPortAsserviceName
        prop.setProperty("driver", "oracle.jdbc.OracleDriver")
      } else if (sqlSelect == "postgresql") {
        outputUrl = "jdbc:" + sqlSelect + "://" + ipAsPortAsserviceName
        prop.setProperty("driver", "org.postgresql.Driver")
      }
      else {
        outputUrl = "jdbc:" + sqlSelect + "://" + ipAsPortAsserviceName
        prop.setProperty("driver", "com.mysql.jdbc.Driver")
      }
      prop.setProperty("user", outputUser)
      prop.setProperty("password", outputPw)
      val table = outTableName
      dfName.write.mode("overwrite").jdbc(outputUrl, table, prop)
      println("finished")

    }

    // 이동평균 함수 정의
    def movingAverage(targetData: Array[Double], myorder: Int): Array[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(x => {
          x.sum
        }).map(x => {
          x / myorder
        })

        if (myorder % 2 == 0) {
          maResult = maResult.sliding(2).map(x => {
            x.sum
          }).map(x => {
            x / 2
          })
        }
        maResult.toArray
      }
    }

    // 변동률 함수 정의
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


    /////// 정제한 QTY값 구하기
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



      // 1. data loading
      //////////////////////////////////////////////////////////////////////////////////////////////////

      var kopoDf = dataLoad("oracle", "kopo", "kopo", "kopo_channel_seasonality_new", "192.168.110.111:1521/orcl", "keydata")


      //////////////////////////////////////////////////////////////////////////////////////////////////
      // 2. data refining
      //////////////////////////////////////////////////////////////////////////////////////////////////

      // keycol은 나중에 파라미터만 키값변경,추가하면 따로 코드수정이 필요하지않음 key값만 주면됌
      var rawData = spark.sql("select concat(a.regionid,'_',a.product) as keycol," +
        "a.regionid as accountid," +
        "a.product," +
        "a.yearweek," +
        "cast(a.qty as String) as qty, " +
        "'test' as productname from keydata a")

      //테이블을 원하는 테이블로 정제한다.
      rawData.show(2)

      var rawDataColumns = rawData.columns.map(x => {
        x.toLowerCase()
      })
      var keyNo = rawDataColumns.indexOf("keycol")
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

      // RDD를 원하는 조건으로 필터링 해준다.
      // 1~52주차 데이터뽑기
      var filterEx1Rdd = rawRdd.filter(x => {

        // Data comes in line by line
        var checkValid = true
        // Assign yearweek information to variables
        var week = x.getString(yearweekNo).substring(4, 6).toInt
        // Assign abnormal to variables
        var standardWeek = 52

        if (week > standardWeek) {
          checkValid = false
        }
        checkValid
      })

      var filterRdd = filterEx1Rdd

      // 필터링한 데이터를 map함수로 데이터값을 변경
      // qtyNo가 700000 이상이면 700000으로 값을 가져온다.
      // key, account, product, yearweek, qty, productname
      var mapRdd = filterRdd.map(x => {
        var qty = x.getString(qtyNo).toDouble
        var maxValue = 700000
        if (qty > 700000) {
          qty = 700000
        }
        Row(x.getString(keyNo),
          x.getString(accountidNo),
          x.getString(productNo),
          x.getString(yearweekNo),
          qty, //x.getString(qtyNo),
          x.getString(productnameNo))
      })

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
      var groupRddMapExp = mapRdd.
        groupBy(x => {
          (x.getString(keyNo))
        }).
        flatMap(x => {

          // key = (A21_PRODUCT1,A21)
          // data = CompactBuffer([A21_PRODUCT1,A21,PRODUCT1,201401,0.0,test], ..엄청많이 들어감
          var key = x._1
          var data = x._2

          // 표준편차구하기
          var size = x._2.size
          var qty = x._2.map(x => {
            x.getDouble(qtyNo)
          }).toArray

          // 1. Sort Data
          var sortedData = data.toSeq.sortBy(x => {
            x.getString(yearweekNo).toInt
          }) // 연주차 오름차순으로 정렬
          var sortedDataIndex = sortedData.zipWithIndex //데이터값에 인덱스추가

          var sortedVolume = sortedData.map(x => {
            (x.getDouble(qtyNo))
          }).toArray // qtyNo만 가져옴 (156개 데이터값)
          var sortedVolumeIndex = sortedVolume.zipWithIndex // 인덱스값이 추가됌 ((0.0,0), (0.0,1)......)

          // 꼭 홀수값만 입력받아야 자기자신값 제외하고 양쪽날개 짝수로 생김
          var scope = 17 //날개 범위 17개
          var subScope = (scope.toDouble / 2.0).floor.toInt // 반쪽날개 범위8개

          // 2. 이동편균함수를 써서 계산한다.
          var movingResult = movingAverage(sortedVolume, scope)

          // 3. Generate value for weeks (out of moving average)
          //    이동평균의 예외범위의 데이터값을 구한다.
          //    scope는 날개 subscope은 날개의 반
          var preMAArray = new ArrayBuffer[Double]
          var postMAArray = new ArrayBuffer[Double]

          // 마지막인덱스는 156
          // for문 디버깅할때는 index값을 임의로 넣어주고 디버깅
          // 임의로 index에 7을 넣어 디버깅
          // 이동평균 앞부분 채우기
          var lastIndex = sortedVolumeIndex.size - 1
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
          var maResult = (preMAArray ++ movingResult ++ postMAArray.reverse).zipWithIndex
          var maResultfinal = (preMAArray ++ movingResult ++ postMAArray.reverse).toArray
          // 5. Generate final-result
          var finalResult = sortedDataIndex.
            zip(maResult).
            map(x => {
              var key = x._1._1.getString(keyNo)
              var regionid = key.split("_")(0)
              var product = key.split("_")(1)
              var yearweek = x._1._1.getString(yearweekNo)
              var volume = x._1._1.getDouble(qtyNo)

              //이동평균구하기
              var movingValue = x._2._1

              // 해당주차비율구하기기
              var ratio = 1.0d
              if (movingValue != 0.0d) {
                ratio = volume / movingValue
              }
              // 평균, 표준편차 계산
              var average = StdStats.mean(movingResult)
              var stddev = stdev(movingResult, 5)

              // 상한,하한
              var upBound = maxValue(stddev, maResultfinal)
              var downBound = minValue(stddev, maResultfinal)

              // 정제된 QTY
              var refinedQty = beegu(sortedVolume, upBound, downBound)

              var week = yearweek.substring(4, 6)
              Row(regionid, product, yearweek, week, volume.toString, movingValue.toString, ratio.toString, average.toString, stddev.toString, upBound.toString, downBound.toString, refinedQty.toString) // 12개
            })
          finalResult

          // 스무딩 구하기
          // 정제된Qty를 가지고 이동평균 구하는것
          var refinedQtyArray = finalResult.map(x => {
            x.getString(11).toDouble
          }).toArray

          var smothingResult = movingAverage(refinedQtyArray, 5).zipWithIndex

          var preMAArray2 = new ArrayBuffer[Double]
          var postMAArray2 = new ArrayBuffer[Double]

          // 마지막인덱스는 156
          // for문 디버깅할때는 index값을 임의로 넣어주고 디버깅
          // 임의로 index에 7을 넣어 디버깅
          var lastIndex2 = smothingResult.size - 1
          subScope = Math.floor(5 / 2).toInt

          for (index <- 0 until subScope) {
            var scopedDataFirst = smothingResult.
              filter(x => x._2 >= 0 && x._2 <= (index + subScope)).
              map(x => {
                x._1
              })
            preMAArray2.append(scopedDataFirst.sum / scopedDataFirst.size
            )
            var scopedDataLast = smothingResult.
              filter(x => {
                ((x._2 >= (lastIndex2 - subScope - index)) &&
                  (x._2 <= lastIndex2))
              }).
              map(x => {
                x._1
              })
            postMAArray2.append(scopedDataLast.sum / scopedDataLast.size)
          }
          var unionArray = preMAArray2 ++ smothingResult.map(x => {
            x._1
          }) ++ postMAArray2.reverse

          var finalResult2 = finalResult.zip(unionArray).map(y => {
            Row(y._1.getString(0), y._1.getString(1), y._1.getString(2), y._1.getString(3), y._1.getString(4), y._1.getString(5), y._1.getString(6), y._1.getString(7), y._1.getString(8), y._1.getString(9), y._1.getString(10), y._1.getString(11), y._2.toString)
          })
          finalResult2
        })
      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data coonversion (RDD -> Dataframe) ///////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      var middleResult = spark.createDataFrame(groupRddMapExp,
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

      var finalResultDf = spark.sql("select A.*,  VOLUME/SMOOTHHANDLING as SEASONALITY, (REFINEDQTY/SMOOTHHANDLING) as SEASONALITY2 from " +
        "(select REGIONID, PRODUCT, WEEK, avg(VOLUME) as VOLUME, avg(MA) as AVG_MA ," +
        " avg(RATIO) as AVG_RATIO , avg(AVERAGE) as AVERAGE , avg(STDDEV) as STDDEV , avg(UPBOUND) as UPBOUND , avg(DOWNBOUND) as DOWNBOUND, avg(REFINEDQTY) as REFINEDQTY ," +
        " avg(SMOOTHHANDLING) as SMOOTHHANDLING " +
        "from (select REGIONID, PRODUCT, WEEK, VOLUME, MA, RATIO, AVERAGE, STDDEV, UPBOUND, DOWNBOUND, REFINEDQTY" +
        ", CASE WHEN SMOOTHHANDLING != 0 then SMOOTHHANDLING " +
        "when SMOOTHHANDLING = 0 then 1" +
        " end as SMOOTHHANDLING from middleTable)  group by REGIONID, PRODUCT, WEEK) A")


      /////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////  Data Unloading        ////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      //    dataOut("postgresql","kopo","kopo","ohmyg","192.168.110.111:5432/kopo",finalResultDf)
      dataOut("oracle", "kopo", "kopo", "ohmyg", "192.168.110.111:1521/orcl", finalResultDf)

      println("Seasonality model completed, Data Inserted in Oracle DB")
  }
}

