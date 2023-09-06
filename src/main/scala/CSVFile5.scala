import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, expr, regexp_replace, substring}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.cloud.spark.bigquery.BigQueryDataFrameReader


object CSVFile5{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CSVJoinApp")
      .set("spark.master", "local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val path = "gs://bucketmal/result/groupedDataset.csv"
    val delimiter = "|"
    val quote = "\""
    val escape = "\\" + quote
    val bucketName = "bucketmal"
    val inputFile1 = "gs://" + bucketName + "/nc_mortality_male.csv"
    val inputFile2 = "gs://" + bucketName + "/suicide_male.csv"
    val inputFile3 = "gs://" + bucketName + "/Metadata_Country.csv"
    val df3:DataFrame = spark.read.option("header", "true").option("quote","\"")
      .csv(inputFile3)
    val df3Count = df3.count()
    println(df3Count)

    //cleanedDf3.withColumn("Country Code", regexp_replace(col("Country Code"), "\"", ""))
    val cleanedDf3 = df3.columns.foldLeft(df3) { (df, colName) =>
      df.withColumn(colName, regexp_replace(col(colName), quote, ""))
    }
    val cleanedDf31 = cleanedDf3
      .select("Country Code", "Region", "IncomeGroup", "SpecialNotes")
    println(cleanedDf3.count())

    cleanedDf3.show(100)


    val df1:DataFrame = spark.read.option("header", "true").option("quote", "\"")
      .csv(inputFile1)//se ha añadido schema
      .select("Country Code","Country Name", "Indicator Name", "Indicator Code","1990", "2000", "2010", "2020")
    val df1Count = df1.count()
    println(df1Count)

    //val cleanedDf1 = df1.withColumn("Country Code", substring(col("Country Code"), 2, 3))//Se ha cambiado en los siguientes cleanedDF1 por df1

    df1.show(100)


    val df2 = spark.read.option("header", "true").csv(inputFile2)
    df2.show()
    df3.show()
    val groupedDF:DataFrame = df1.join(cleanedDf31, Seq("Country Code"))//Antes el join era con cleanedDf3
    groupedDF.show()
    val repartitionedDF = groupedDF.coalesce(1)//Se ha cambiado el groupedDF siguiente por repartitionedDF-->Intentar jugar con particiones de spark
      .withColumnRenamed("Country Code", "countryCode")
      .withColumnRenamed("Country Name", "countryName")
      .withColumnRenamed("Indicator Name", "indicatorName")
      .withColumnRenamed("Indicator Code", "indicatorCode")
    repartitionedDF.write.mode("overwrite").format("csv").option("header", "true").option("quote","").option("emptyValue","").save(path)

    repartitionedDF.write
      .format("bigquery")
      .mode("overwrite")
      .option("table", "model-journal-395918.dataset1Metadata.countryTable")
      .option("temporaryGcsBucket", "bucketmal")//"gs://bucketmal"-->rename columns
      .save()
    spark.stop()
  }
}
