package topicmodeling


import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.EMLDAOptimizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class PLSA (val preparedDF: DataFrame, val spark: SparkSession)  {

  def buildVocabulary() :DataFrame = {

   val wordCountDF = preparedDF
     .select(explode_outer(preparedDF("prepared")).alias("word"))
      .groupBy("word").count()
      .orderBy(desc("count"))
    val fill = array().cast("array<string>")
  val prep  = when(preparedDF("prepared").isNull, fill).otherwise(preparedDF("prepared"))
    val newDF = preparedDF.withColumn("prepared", prep)
    val cvModel = new CountVectorizer()
      .setInputCol("prepared")
      .setOutputCol("features")
      .setMinDF(2)
      .fit(preparedDF)

    val vectorizeDF : DataFrame = cvModel.transform(preparedDF)
    val optimazer = new EMLDAOptimizer
    val lda = new LDA()
      .setK(5)
      .fit(vectorizeDF)
    val vocabulary = spark
      .sparkContext
      .broadcast(cvModel.vocabulary)
    val wordsWithWeights = udf( (x : mutable.WrappedArray[Int],
                                 y : mutable.WrappedArray[Double]) =>
    { x.map(i => vocabulary.value(i)).zip(y)}
    )

    val TopicsDF = lda
        .describeTopics(5)
      .withColumn("topicWords",
        wordsWithWeights(col("termIndices"), col("termWeights")))
      .select("topic", "topicWords")
      .withColumn("topicWords", explode(col("topicWords")))
    val wordsToTopicDF = TopicsDF
      .select(
        col("topic"),
        col("topicWords").getField("_1").as("word"),
        col("topicWords").getField("_2").as("weight")
      )
    wordsToTopicDF
  }

}
