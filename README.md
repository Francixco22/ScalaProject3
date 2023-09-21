# ScalaProject3
En este repositorio está el proyecto de Scala para el primer ETL desarrollado en IntellIJ.

Además del código en Scala, las 2 funciones .py utlizadas se encuentran en la carpeta Python del proyecto. APIToPubSub es la función para leer los datos de la API de Red Eléctrica y subirlos al broker de Pubsub,
mientras que CSVToBigQuery sirve para leer los mensajes de Pubsub y escribirlos en BigQuery y en Cloud Storage.

Los archivos utilizados en el primer ETL se encuentran en src -> main -> scala -> Files. Los objetos .scala utilizados en este trabajo se encuentran en la carpeta src -> main -> scala, siendo UploadData.scala el objeto que sube los archivos desde la carpeta Files al bucket de Cloud Storage y CSVFilePivot.scala el objeto que realiza todo el procesado y escritura de datos.
