import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import java.io.{File, FileInputStream}
import com.google.auth.oauth2.ServiceAccountCredentials

object UploadData {
  def main(args: Array[String]): Unit = {

    val projectId = "model-journal-395918"
    val bucketName = "bucketresult"
    val localFilePath = "C:/Users/USUARIO/IdeaProjects/ScalaProject/src/main/scala/Files"
    val objectName = "uploaded-file.txt"
    val credentialsStream = new FileInputStream("C:/Users/USUARIO/IdeaProjects/ScalaProject/src/main/scala/key/clave.json")
    val credentials = ServiceAccountCredentials.fromStream(credentialsStream)
    val storage: Storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService()

    val localFolder = new File(localFilePath)
    val filesInFolder = localFolder.listFiles()

    filesInFolder.foreach { file =>
      val objectName = file.getName
      val blobId = com.google.cloud.storage.BlobId.of(bucketName, objectName)
      val blobInfo = BlobInfo.newBuilder(blobId).build()
      val blob = storage.create(blobInfo, new FileInputStream(file))
      println(s"File uploaded to GCS bucket: gs://$bucketName/$objectName")
    }
  }
}
