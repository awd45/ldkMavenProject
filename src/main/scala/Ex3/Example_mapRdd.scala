package Ex3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType,StructField, StructType}
import org.apache.spark.sql.Row

object Example_mapRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("...").
      config("spark.master", "local").
      getOrCreate()


    var MAXVALUE = 700000

    /////////////////////////////

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("product_name")

    // (kecol, accountid, product, yearweek, qty, product_name)
    var rawRdd = rawData.rdd

    var filteredRdd = rawRdd.filter(x => {
      // boolean = true
      var checkValid = true
      // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
      if (weekValue >= 53) {
        checkValid = false
      }

      checkValid
    })
    // filteredRdd.first
    // filteredRdd = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)

    // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.


    var mapRdd = filteredRdd.map(x => {

      // 디버깅코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000 }).first
      //로직구현예정

      var org_qty = x.getDouble(qtyNo)
      var new_qty = org_qty

      if (new_qty > MAXVALUE) {
        new_qty = MAXVALUE
      }

      //출력 row 키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new )
      Row(x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty
      )
    })
    // 그냥 mapRdd는 데이터가 겹치느는 건 사라짐
    var groupRdd1 = mapRdd.groupBy(x => {
      (x.getString(keyNo))
    }).map(x => {
      //key 값
      var key1 = x._1
      //기본데이터
      var data1 = x._2
      //각종 살을 덫붙이는것
      var dataSize = data1.size
      (key1, dataSize)

      //groupRdd1.foreach(println) 하면 그룹맵핑된 결과나옴

      //출력 row 키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new)
      // (거래처/상품) 0        1             2             3

      //flatmap은 데이터가 겹쳐도 그대로 살아있음
      // flatmap data 그대로 살아있고, group data로 연산도 가능
      var groupRdd2 = mapRdd.groupBy(x => {
        (x.getString(keyNo))
      }).flatMap(x => {
        // key 값
        var key2 = x._1
        // 기본데이터
        var data2 = x._2

        var dataSize = data2.size

        var sumation = data2.map(x => {
          x.getDouble(3)
        }).sum

        var average = 0.0
        if (dataSize == 0) {
          average = 0.0d
        } else {
          average = sumation / dataSize
        }
        var outputData = data2.map(x => {
          Row(x.getString(0), // key 정보 (거래처, 상품)
            x.getString(1), // 주차 정보(yearweek)
            x.getDouble(2), // original qty
            x.getDouble(3), // revistion qty
            average //key(거래처, 상품)의 평균
          )
        })
        outputData
      })
      //groupRdd1.foreach(println)하면 그냥 map과 flatmap의 차이를 알 수 있음
    })
  }
}