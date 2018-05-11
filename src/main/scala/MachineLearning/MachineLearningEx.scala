package MachineLearning

object MachineLearningEx {
  def main(args: Array[String]): Unit = {
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////  Library Definition ////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    import org.apache.spark.sql.SparkSession

    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
    import org.apache.spark.ml.regression._//{DecisionTreeRegressionModel, DecisionTreeRegressor}
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.sql.functions._

    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    //https://spark.apache.org/docs/latest/configuration.html
    // import
    // Checkpoint mllib and spark version shall be same

    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////
    // Data Loading
    ///////////////////////////
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    val decisionIn = spark.read.format("jdbc").options(Map("url" -> staticUrl, "dbtable" -> "KOPO_DECISION_TREE_EXAMPLE", "user" -> staticUser, "password" -> staticPw)).load
    decisionIn.createOrReplaceTempView("decisionInputTable")

    var predictStd = "201630"

    val decisionTreeInputDF = spark.sql("select regionid, productgroup, product, item, yearweek, year, cast(week as double) as week , " +
      "cast(qty as double), holiday, hclus, " +
      "promotion" +
      ", cast(pro_percent as double)," +
      "case when promotion = 'Y' then 1 else 0 end as promotioncode," +
      "case when holiday = 'Y' then 1 else 0 end as holidaycode" +
      " from decisionInputTable" +
      " where yearweek between '201501' and '201652'")

    ///////////////////////////
    // Make Training-Set
    ///////////////////////////
    val train = decisionTreeInputDF.filter("yearweek <= " + predictStd)
    //var train = decisionTreeInputDF.filter(x=>{x.getString(4).toInt <= 201630 })
    println(train.show(2))

    ///////////////////////////
    // Make Test-Set
    ///////////////////////////
    val test = decisionTreeInputDF.filter("yearweek > " + predictStd)
    println(test.show(2))

    ///////////////////////////
    // Training
    ///////////////////////////
    // Set the independent variables
    var assembler = new VectorAssembler().setInputCols(Array("week", "pro_percent", "holidaycode")).setOutputCol("features")
    // Set the dependent variables
    var dt = new DecisionTreeRegressor().setLabelCol("qty").setFeaturesCol("features")
    val pipeline = new Pipeline().setStages(Array(assembler, dt))
    val model = pipeline.fit(train)

    ///////////////////////////
    // Prediction
    ///////////////////////////
    val predictions = model.transform(test)
    predictions.orderBy(asc("yearweek")).show

    ///////////////////////////
    // Validation
    ///////////////////////////
    val evaluatorRmse = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("rmse")
    val evaluatorMae = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("mae")
    val rmse = evaluatorRmse.evaluate(predictions)
    val mae = evaluatorMae.evaluate(predictions)
    println("Decision Tree Root Mean Squared Error (RMSE) on test data = " + rmse)
    println("Decision Tree Mean Average Error (MAE) on test data = " + mae)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)

    ///////////////////////////
    // Transpose & Save data
    ///////////////////////////
    var middleResult = predictions.createOrReplaceTempView("decisionresult")

    var finalResult = spark.sql(
      ("select 'real-qty' as measure," +
        "productgroup," +
        "product," +
        "item," +
        "yearweek," +
        "qty as sales from decisionresult " +
        "union all " +
        "select 'prediction-qty' as measure," +
        "productgroup," +
        "product," +
        "item," +
        "yearweek," +
        "prediction as sales from decisionresult").toUpperCase)

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "dt_result_final2"
    finalResult.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("Decisiontree oracle import completed")

    //////////////////////////////////////////Others - Linear Regression //////////////////////////////////////////
    ///////////////////////////
    // Training
    ///////////////////////////
    // Set the independent variables
    var assembler2 = new VectorAssembler().setInputCols(Array("week", "pro_percent", "holidaycode")).setOutputCol("features")
    // Set the dependent variables
    var dt2 = new LinearRegression().setLabelCol("qty").setFeaturesCol("features")
    val pipeline2 = new Pipeline().setStages(Array(assembler2, dt2))
    val model2 = pipeline2.fit(train)

    ///////////////////////////
    // Prediction
    ///////////////////////////
    val predictions2 = model2.transform(test)
    predictions2.orderBy(asc("yearweek")).show

    ///////////////////////////
    // Validation
    ///////////////////////////
    val evaluatorRmse2 = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("rmse")
    val evaluatorMae2 = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("mae")
    val rmse2 = evaluatorRmse2.evaluate(predictions2)
    val mae2 = evaluatorMae2.evaluate(predictions2)
    println("Linear Regression Root Mean Squared Error (RMSE) on test data = " + rmse2)
    println("Linear Regression  Mean Average Error (MAE) on test data = " + mae2)

    //////////////////////////////////////////Others - Random Foreset //////////////////////////////////////////
    ///////////////////////////
    // Training
    ///////////////////////////
    // Set the independent variables
    var assembler3 = new VectorAssembler().setInputCols(Array("week", "pro_percent", "holidaycode")).setOutputCol("features")
    // Set the dependent variables
    var dt3 = new RandomForestRegressor().setLabelCol("qty").setFeaturesCol("features")
    val pipeline3 = new Pipeline().setStages(Array(assembler3, dt3))
    val model3 = pipeline3.fit(train)

    ///////////////////////////
    // Prediction
    ///////////////////////////
    val predictions3 = model3.transform(test)
    predictions3.orderBy(asc("yearweek")).show

    ///////////////////////////
    // Validation
    ///////////////////////////
    val evaluatorRmse3 = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("rmse")
    val evaluatorMae3 = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("mae")
    val rmse3 = evaluatorRmse3.evaluate(predictions3)
    val mae3 = evaluatorMae3.evaluate(predictions3)

    println("Decision Tree Root Mean Squared Error (RMSE) on test data = " + rmse)
    println("Decision Tree Mean Average Error (MAE) on test data = " + mae)
    println("Linear Regression Root Mean Squared Error (RMSE) on test data = " + rmse2)
    println("Linear Regression  Mean Average Error (MAE) on test data = " + mae2)
    println("Random Forest Root Mean Squared Error (RMSE) on test data = " + rmse3)
    println("Random Forest  Mean Average Error (MAE) on test data = " + mae3)
  }
}


