// encoding: UTF-8

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVFilePivot {
  /*En primer lugar se definen 2 funciones: una para cambiar el nombre y preparar las columnas del DataFrame para el
  * pivoting(changeName), y otra que realizara el proceso de unpivoting para realizar el metod unpivot al DataFrame de
  * interes*/
  def changeName(df:DataFrame,name:String) = df.withColumnRenamed("Country Name", "Country")
    .withColumnRenamed("Country Code", "Code")
    .withColumnRenamed("Indicator Name", name)

  def unpivoting(n:Int, name: String, expresion:String, df: DataFrame):DataFrame =
    df.selectExpr("Country", "Code", s"stack($n, $expresion) as (Year, "+name+")")

  def main(args: Array[String]): Unit = {
    //Creacion de una nueva sesion de Spark
    val conf = new SparkConf().setAppName("CSVJoinApp")
      .set("spark.master", "local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //Definicion de los paths para la lectura de los datos en el bucket de Google Cloud Storage
    val path4 = "gs://bucketmal/result/groupedDatasetSF.csv"
    val quote = "\""
    val bucketName = "bucketmal"
    val inputFile1 = "gs://" + bucketName + "/nc_mortality_male.csv"
    val inputFile2 = "gs://" + bucketName + "/suicide_male.csv"
    val inputFile5 = "gs://" + bucketName + "/nc_mortality_female.csv"
    val inputFile4 = "gs://" + bucketName + "/suicide_female.csv"
    val inputFile6 = "gs://" + bucketName + "/Medical_doctors.csv"
    val inputFile3 = "gs://" + bucketName + "/Metadata_Country.csv"
    //Lectura de los archivos en el bucket
    val df1: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile1)
    println(df1.count())
    df1.show()
    val df11: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile2)
    val df12: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile5)
    val df13: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile4)
    val df3: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile3)
    val dfMD: DataFrame = spark.read.option("header", "true").option("quote", "\"").csv(inputFile6)
    //Reasignacion de los nombres de algunas variables para facilitar el proceso de unpivoting para que por cada columna
    //que representa un year se incluya una fila mas por cada pais diferente
    //Hasta aqui la bronze layer
    //Comienzo de la silver layer
    val df2 = changeName(df1, "MortalityMale")
    val df21 = changeName(df11, "SuicideMale")
    val df22 = changeName(df12, "MortalityFemale")
    val df23 = changeName(df13, "SuicideFemale")

    //Definicion de las variables del dataset sobre las cuales se realizara el unpivoting
    val yearColumns = Seq("1960", "1961", "1962", "1963", "1964", "1965", "1967", "1968", "1969", "1970", "1971",
      "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979", "1980", "1981", "1982", "1983", "1984", "1985",
      "1986", "1987", "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999",
      "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013",
      "2014", "2015", "2016", "2017", "2018", "2019", "2020")

    val yearExpressions = yearColumns.flatMap { year =>
      Seq(s"'$year'", s"`$year`")
    }
    val stackExpression = yearExpressions.mkString(", ")

    //Se realiza el unpivoting en todos los datasets de interes en funcion del numero de years definidos en yearColumns y
    //la expresion definida en yearExpressions y stackExpression
    val unpivoted_df = unpivoting(yearColumns.length, "MortalityMale", stackExpression, df2)
    unpivoted_df.show()
    println(unpivoted_df.count())
    val unpivoted_df2 = unpivoting(yearColumns.length, "SuicideMale", stackExpression, df21)
    val unpivoted_df3 = unpivoting(yearColumns.length, "MortalityFemale", stackExpression, df22)
    val unpivoted_df4 = unpivoting(yearColumns.length, "SuicideFemale", stackExpression, df23)

    //Lectura de un dataset con informacion importante sobre factores de los paises como la region a la que pertenecen
    //y el nivel de ingresos que tienen para complementar la informacion de los datos de mortalidad y suicidio
    val cleanedDf3 = df3.columns.foldLeft(df3) { (df, colName) =>
      df.withColumn(colName, regexp_replace(col(colName), quote, ""))
    }
    val cleanedDf31 = cleanedDf3
      .select("Country Code", "Region", "IncomeGroup") //Se ha eliminado SpecialNotes
      .withColumnRenamed("Country Code", "Code")

    val groupedDF1: DataFrame = unpivoted_df.join(cleanedDf31, Seq("Code"))
    groupedDF1.show()
    val groupedDF2: DataFrame = groupedDF1.join(unpivoted_df2, Seq("Code", "Year", "Country"))
    groupedDF2.show()
    val groupedDF3: DataFrame = groupedDF2.join(unpivoted_df3, Seq("Code", "Year", "Country"))
    val groupedDF4: DataFrame = groupedDF3.join(unpivoted_df4, Seq("Code", "Year", "Country"))
    //Join con el dataset de medicos para proporcionar informacion adicional al dataset de trabajo
    val finalDF: DataFrame = groupedDF4.join(dfMD, Seq("Country", "Year"), "Left")
    println(finalDF.count())
    finalDF.show()
    /*En el dataset final hay muchas filas que presentan valores nulos en las 4 variables de interes (MortalityMale,
    MortalityFemale, SuicideMale, SuicideFemale), por lo que es necesario que las filas con los 4 valores nulos se
    eliminen, esto se realizara mediante un proceso filter*/
    val columnsToCheck = Seq("MortalityMale", "MortalityFemale", "SuicideMale", "SuicideFemale")
    val filteredDF = finalDF.filter(columnsToCheck.map(colName => col(colName).isNotNull).reduce(_ || _))
    //Hasta aqui silver layer
    //Comienzo de la gold layer
    //En primer lugar se escribira en el bucket bucketmal el .csv de los datos tras realizar los joins y el filtrado
    filteredDF.write.mode("overwrite").format("csv").option("header", "true").option("quote", "")
      .option("emptyValue", "").save(path4)
    /*El siguiente paso es escribir los datos en BigQuery, para ello, hay que modificar los nombres de algunas columnas
    * debido a que estas contienen caracteres que BigQuery no puede procesar*/
    val filtered2 = filteredDF.withColumnRenamed("Medical doctors (per 10 000 population)", "DoctorsPer10000")
      .withColumnRenamed("Medical doctors (number)", "numberMedicalDoctors")
      .withColumnRenamed("Generalist medical practitioners (number)", "numberGeneralistMedicalPractitioners")
      .withColumnRenamed("Specialist medical practitioners (number)", "numberSpecialistMedicalPractitioners")
      .withColumnRenamed("Medical doctors not further defined (number)", "numberMedicalDoctorsNotDefined")
    /*El ultimo paso sera especificar que tipo de variable debe ser cada columna, puesto que BigQuery escribe todas las
    * columnas como STRING, por lo que habra que convertir las columnas que se correspondan con valores numericos a
    * FLOATTYPE*/
    val filtered3 = filtered2.withColumn("MortalityMale", col("MortalityMale").cast(FloatType))
      .withColumn("MortalityFemale", col("MortalityFemale").cast(FloatType))
      .withColumn("SuicideMale", col("SuicideMale").cast(FloatType))
      .withColumn("SuicideFemale", col("SuicideFemale").cast(FloatType))
      .withColumn("numberMedicalDoctors", col("numberMedicalDoctors").cast(FloatType))
      .withColumn("DoctorsPer10000", col("DoctorsPer10000").cast(FloatType))
      .withColumn("numberSpecialistMedicalPractitioners",
        col("numberSpecialistMedicalPractitioners").cast(FloatType))
      .withColumn("numberGeneralistMedicalPractitioners",
        col("numberGeneralistMedicalPractitioners").cast(FloatType))
      .withColumn("numberMedicalDoctorsNotDefined",
        col("numberMedicalDoctorsNotDefined").cast(FloatType))
    //Una vez el procesado ha terminado, se escribira el dataframe resultante en BigQuery
    println(filtered3.count())
    filtered3.show()
    filtered3.write
      .format("bigquery")
      .mode("overwrite")
      .option("table", "model-journal-395918.dataset1Metadata.countryTable")
      .option("temporaryGcsBucket", "bucketmal")
      .save()
    spark.stop()
}
}
