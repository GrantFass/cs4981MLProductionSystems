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
//    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "training_service")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "railroad-QUAGMIRE-leaf")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
//    spark.sparkContext
//      .hadoopConfiguration.set("hadoop.home.dir", "C:\\lwshare\\hadoopHomeDir")

    //<editor-fold desc="Read Spam Email Classification From Log File">

    val documents = spark.read
      // The below argument specifies reading the entire document as a string instead of line by line
      //      .option("wholetext", true)
      .json(
        "s3a://log-files/*"
      )

    // get the number of documents. If line by line this is the number of lines.
    val nDocuments = documents.count()

    println(s"Read ${nDocuments} documents")
    println()

    // only get rows with spam label
    val spam = documents
      // transformation
      .filter(documents("label") === "spam")

    //</editor-fold>

    //<editor-fold desc="Retrieve Postgres Emails">
    // https://github.com/rnowling/spark-examples/blob/main/src/main/scala/SQLDatabaseExample.scala

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

    //</editor-fold>

    //<editor-fold desc="Join The Two Datasets">

    //    println("spam table:")
    //    spam.show(5)

    // the first below line is the one that is from the slides but ends up duplicating the email_id col.
    //    var spam_emails = tableDf.join(spam, tableDf("email_id")===spam("email_id"), "left_outer")
    var spam_emails = tableDf.join(spam, Seq("email_id"), "left_outer")
    // replace nulls with ham
    spam_emails = spam_emails.na.fill("ham", Array("label"))

        println(spam_emails)
        spam_emails.show(20)

    //    val count = tableDf.count();

    //    println(s"There are ${count} rows")

    //</editor-fold>


    //<editor-fold desc="Store Labeled Emails In Object Store">

    // spam_emails is the data
    // bucket is joined-out
    spam_emails.write.mode("overwrite").json("s3a://joined-out/out")
//    spam_emails.write.mode("overwrite").json("C:\\Users\\fassg\\Downloads\\out.json")

//    val documents = spark.read
//      // The below argument specifies reading the entire document as a string instead of line by line
//      //      .option("wholetext", true)
//      .json(
//        "s3a://log-files/*"
//      )


    //</editor-fold>

    spark.stop()
    // the below two lines launch minio and postgres which are necessary to run this file
    // additionally if you are on windows you will run into hadoop errors. These can be solved in the following steps.
    // 1. Install hadoop. Since you are on windows I recommend using chocolaty then do cinst hadoop.
    //     This installs hadoop 3.3.4. It also has the benefit of seeting your enviornment vars for you
    // 2. Go to this link https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin and download everything
    // 3. extract the stuff you downloaded and copy everything in the hadoop-3.2.2/bin folder
    // 4. go to your hadoop install location locally. This should be under c://hadoop/hadoop-3.3.4
    // 5. dump the contents of the folder you copied earlier into the bin folder under the directory you just navigated to.
    // 6. DO NOT replace files with the same name.
    // 7. Once this finishes copy the hadoop.dll and winutils.exe files
    // 8. navigate to c://windows/system32 and paste the two files you just copied.
    // 9. Assuming you prayed to every god you know of and sacrificed your firstborn you may have a 10% chance of it working.
    // psql -U postgres
    // minio server minio_data
  }
}
