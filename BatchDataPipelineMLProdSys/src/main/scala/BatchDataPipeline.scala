import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col

import java.util.Properties
import java.util

object BatchDataPipeline {

  def extractWords(document: String): Seq[String] = {
    document.toLowerCase
      // remove anything that isn't a letter or whitespace
      .map(c => if (c.isLetter || c.isWhitespace) c else ' ')
      // split words on any whitespace
      .split("\\s+")
  }

  // Same definition of main as in Java
  // Takes an Array of strings and returns nothing (Unit)
  def main(args: Array[String]): Unit = {
    // The Spark system is started by creating a SparkSession
    // object.  This object is used to create and register
    // all distributed collections.
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[4]")
      .appName("BatchDataPipelineMLProdSys")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    //<editor-fold desc="Read Spam Email Classification From Log File">
    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    // read in the specified document
    val documents = spark.read
      // The below argument specifies reading the entire document as a string instead of line by line
//      .option("wholetext", true)
      .textFile(
        "C:\\Users\\garciara\\Projects\\cs4981MLProductionSystems\\log_file.json"
      )

    // get the number of documents. If line by line this is the number of lines.
    // action
    val nDocuments = documents.count()

    println(s"Read ${nDocuments} documents")
    println()

    // Generate the list of all of the lines that contain the word "spam"
    val spam = documents
      // transformation
      .filter(doc => doc.contains("spam"))

    // print out the spam lines if you want to check
//    println("spam emails:");
//    for (doc <- spam) {
//      println(s"${doc}")
//      println("----------------------------------------")
//    }
    //</editor-fold>

    //<editor-fold desc="Retrieve Postgres Emails">
    // https://github.com/rnowling/spark-examples/blob/main/src/main/scala/SQLDatabaseExample.scala

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    // database connection details
    val url = "jdbc:postgresql://127.0.0.1:5432/email_ingestion"
    val tableName = "emails"
    val props = new Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "secret_password")

    // training_service
    // railroad-QUAGMIRE-leaf

    // read table into DataFrame
    var tableDf = spark.read
      .jdbc(url, tableName, props)
    tableDf = tableDf.withColumn("spam_label", lit(0))
    var spam_emails = tableDf.join(spam, "email_id")

//    tableDf.map(row => {
//      if(col("")
//    })

    tableDf.show()

    val count = tableDf.count();

    println(s"There are ${count} rows")
    //</editor-fold>



    //<editor-fold desc="Join The Two Datasets">


    // spam is the spam list
    // tableDf is the postgres data

    
    //</editor-fold>


    //<editor-fold desc="Store Labeled Emails In Object Store">

    // temp



    //</editor-fold>

    spark.stop()
  }
}
