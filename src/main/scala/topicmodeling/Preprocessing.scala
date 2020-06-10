package topicmodeling
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame

class Preprocessing(val questions: DataFrame, val colName: String) {

  val regexTokenizer = new RegexTokenizer()
    .setInputCol(colName)
    .setMinTokenLength(3)
    .setOutputCol("tokens")
    .setPattern("\\W+")
    .setGaps(true)
//  val document = new DocumentAssembler().setInputCol("Body").setOutputCol("document")
// val sentence: SentenceDetector = new SentenceDetector()
//    .setInputCols(Array("document"))
//    .setOutputCol("sentence")
//   .setMaxLength(10)
//  val tokenizer: Tokenizer = new Tokenizer()
//    .setContextChars(Array("(", ")", "?", "!"))
//    .setInputCols(Array("sentence"))
//    .setOutputCol("token")

  val remover = new StopWordsRemover()
    .setInputCol("tokens")
    .setOutputCol("prepared")
//  val pipeline: Pipeline = new Pipeline()
//    .setStages(Array(
//      document,
//      sentence,
//      tokenizer
//    ))
   val tokenizeDF = regexTokenizer.transform(questions)
  val prepareDF: DataFrame = remover.transform(tokenizeDF)
//  val pipelineDF: DataFrame = pipeline.fit(questions).transform(questions)


}
