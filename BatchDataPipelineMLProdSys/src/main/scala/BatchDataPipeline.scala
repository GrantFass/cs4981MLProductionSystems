import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when

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
      .json(
        "C:\\lwshare\\cs4981MLProdSys\\log_file.json"
      )

    // get the number of documents. If line by line this is the number of lines.
    // action
    val nDocuments = documents.count()

    println(s"Read ${nDocuments} documents")
    println()

//    println("documents")

//    documents.show(20)
    // Generate the list of all of the lines that contain the word "spam"
    val spam = documents
      // transformation
      .filter(documents("label") === "spam")

//    spam.show(5)


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
//    props.setProperty("password", "secret_password")
    props.setProperty("password", "5432")

    // training_service
    // railroad-QUAGMIRE-leaf

    // read table into DataFrame
    var tableDf = spark.read
      .jdbc(url, tableName, props)
//    tableDf = tableDf.withColumn("spam_label", lit(0))
    println("spam table:")
    spam.show(5)

//    var spam_emails = tableDf.join(spam, "email_id")
    // need left outer join

    // the first below line is the one that is from the slides but ends up duplicating the email_id col.
//    var spam_emails = tableDf.join(spam, tableDf("email_id")===spam("email_id"), "left_outer")
    var spam_emails = tableDf.join(spam, Seq("email_id"), "left_outer")


//    spam_emails = when(spam_emails("label") === "spam", "spam").otherwise("ham")
//    spam_emails.select(col("*"), when(col("label") === "spam", "spam").otherwise("ham"))
    spam_emails = spam_emails.na.fill("ham", Array("label"))

//    var spam_emails = tableDf.join(spam, "email_id", "left_outer")
//    var df1 = tableDf
//    var df2 = spam
//    df1.join(df2,
//      df1("email_id") == df2("email_id"),
//      "left_outer")


    println(spam_emails)
    spam_emails.show(20)

//    tableDf.map(row => {
//      if(col("")
//    })
//    println("tabledf table:")
//    tableDf.show(5)

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
