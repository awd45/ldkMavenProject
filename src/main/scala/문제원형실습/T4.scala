package 문제원형실습

object T4 {

    def main(args: Array[String]): Unit = {

      import org.apache.spark.sql.SparkSession
      import scala.collection.mutable.ArrayBuffer
      import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
      import org.apache.spark.sql.types.{StringType, StructField, StructType}

      var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

      // 1. Data Loading (from Oracle DB)

      var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
      var staticUser = "kopo"
      var staticPw = "kopo"
      var selloutDb = "kopo_channel_seasonality_new"

      val selloutDataFromOracle = spark.read.format("jdbc").options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

      selloutDataFromOracle.createOrReplaceTempView("rawData")

      println(selloutDataFromOracle.show())

      // 2. Funtion, Query

      def wingLength(window: Int): Int = {
        if (window % 2 == 0) {
          throw new IllegalArgumentException("Only an odd number, please.")
        }
        var wing = (window - 1) / 2
        wing
      }

      var window1 = 17 //  Need to be connected to the parameter map

      var window2 = 5

      // window1 = spark.sql("SELECT PARAM_VALUE FROM T3_PARAMETER_MAP " +
      //						"WHERE 1=1 " +
      //						"AND CATEGORY IN ('COMMON', 'MODEL_1') " +
      //						"AND PARAM_NAME = WINDOW1 " +
      //						"AND USE_YN = 'Y' ")

      // window2 = spark.sql("SELECT PARAM_VALUE FROM T3_PARAMETER_MAP " +
      //						"WHERE 1=1 " +
      //						"AND CATEGORY IN ('COMMON', 'MODEL_1') " +
      //						"AND PARAM_NAME = WINDOW2 " +
      //						"AND USE_YN = 'Y' ")

      var wing1 = wingLength(window1).toString() // i.e. 8

      var wing2 = wingLength(window2).toString() //  i.e. 2

      var validWeek = 52

      // validWeek = spark.sql("SELECT PARAM_VALUE FROM T3_PARAMETER_MAP " +
      //						"WHERE 1=1 " +
      //						"AND CATEGORY IN ('COMMON', 'MODEL_1') " +
      //						"AND PARAM_NAME = VALID_WEEK " +
      //						"AND USE_YN = 'Y' ")

      var query_rolling1 = """OVER (PARTITION BY REGIONID, PRODUCT
                        ORDER BY REGIONID, PRODUCT, YEARWEEK
                        ROWS BETWEEN """ + wing1 + " PRECEDING AND " + wing1 + " FOLLOWING) "

      var query_rolling2 = """OVER (PARTITION BY REGIONID, PRODUCT
                        ORDER BY REGIONID, PRODUCT, YEARWEEK
                        ROWS BETWEEN """ + wing2 + " PRECEDING AND " + wing2 + " FOLLOWING) "

      // 3. Spark SQL Query

      var query_cleansing = """SELECT REGIONID
                        ,PRODUCT
                        ,YEARWEEK
                        ,SUBSTR(YEARWEEK,5,2) AS WEEK
                        ,CASE WHEN QTY<0 THEN 0
                        ELSE QTY END AS QTY
                        FROM rawData
                        WHERE SUBSTR(YEARWEEK,5,2) <= """ + validWeek.toString()

      var data_cleansed = spark.sql(query_cleansing)

      data_cleansed.createOrReplaceTempView("data_cleansed")
      data_cleansed.show(2)

      var query_ma = "SELECT A.*, STDDEV(MA)" + query_rolling2 + """ AS STDDEV
                  FROM ( SELECT REGIONID
                    ,PRODUCT
                    ,YEARWEEK
                    ,QTY
                    ,AVG(QTY) """ + query_rolling1 + " AS MA " + "FROM data_cleansed) A"

      var data_ma = spark.sql(query_ma)

      data_ma.createOrReplaceTempView("data_ma")
      data_ma.show(2)

      var query_smoothing = "SELECT A.* " + ",AVG(REFINED) " + query_rolling2 + """ AS SMOOTHED
                          FROM ( SELECT REGIONID
                            ,PRODUCT
                            ,YEARWEEK
                            ,QTY
                            ,MA
                            ,STDDEV
                            ,(MA + STDDEV) AS UPPER_BOUND
                            ,(MA - STDDEV) AS LOWER_BOUND
                            ,CASE WHEN QTY > (MA + STDDEV) THEN (MA + STDDEV)
                                WHEN QTY < (MA - STDDEV) THEN (MA - STDDEV)
                                ELSE QTY END AS REFINED FROM data_ma) A"""

      var data_smoothed = spark.sql(query_smoothing)

      data_smoothed.createOrReplaceTempView("data_smoothed")
      data_smoothed.show(2)

      var query_seasonal = """SELECT A.*
                           ,CASE WHEN SMOOTHED = 0 THEN 1
                               ELSE (QTY / SMOOTHED) END AS S_INDEX_1
                           ,CASE WHEN SMOOTHED = 0 THEN 1
                               ELSE (REFINED / SMOOTHED) END AS S_INDEX_2
                           FROM data_smoothed A"""

      var data_seasonal = spark.sql(query_seasonal)

      data_seasonal.createOrReplaceTempView("data_seasonal")
      data_seasonal.show(2)


      var query_seasonal_final = """SELECT REGIONID
                                  ,PRODUCT
                                  ,SUBSTR(YEARWEEK, 5, 2) AS WEEK
                                  ,AVG(S_INDEX_1) AS S_INDEX_1
                                  ,AVG(S_INDEX_2) AS S_INDEX_2
                                FROM data_seasonal
                                GROUP BY REGIONID, PRODUCT, SUBSTR(YEARWEEK, 5, 2)"""

      var data_seasonal_final = spark.sql(query_seasonal_final)

      data_seasonal_final.createOrReplaceTempView("data_seasonal_final")
      data_seasonal_final.show(2)

      // 4. Data Unloading (

      val prop = new java.util.Properties
      prop.setProperty("driver", "oracle.jdbc.OracleDriver")
      prop.setProperty("user", staticUser)
      prop.setProperty("password", staticPw)
      val table = "t3_seasonality_index"

      data_seasonal_final.write.mode("overwrite").jdbc(staticUrl, table, prop)
      println("Seasonality model completed, Data Inserted in Oracle DB")

    }

  }
