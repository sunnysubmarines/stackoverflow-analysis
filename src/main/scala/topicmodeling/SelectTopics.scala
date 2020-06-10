package topicmodeling

import org.apache.spark.sql.SparkSession

object SelectTopics {
  def main(args: Array[String]): Unit = {
    val inputPath: String = "data example/answers.csv"
    val spark = SparkSession.
      builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val df = spark
      .read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(inputPath)
    val prepare = new Preprocessing(df, "tags")

    val prepareDF = prepare.prepareDF
    prepareDF.printSchema()

    // val exp = prepareDF.withColumn("prepared", explode_outer($"prepared"))
    //exp.show

    val topicsDF = new PLSA(prepareDF,spark).buildVocabulary()
    topicsDF.show()
    topicsDF
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("data example/topics.csv")



  }
}
