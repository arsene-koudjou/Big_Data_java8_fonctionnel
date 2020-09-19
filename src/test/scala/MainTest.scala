
import java.io.File

import com.typesafe.config.ConfigFactory
import fr.aretex.irex.hdfs.{JavaMain, Main}
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.assertj.core.api.Assertions.assertThat

@RunWith(classOf[JUnitRunner])
class MainTest extends FunSuite with BeforeAndAfter with Serializable {

  val config = ConfigFactory.load("app.properties")
  val outputPath = config.getString("app.data.output")

  before {
    // build SparkContext
    println(s"Cleaning ${outputPath.replaceFirst("file:///", "")}")
    FileUtils.deleteDirectory(new File(outputPath.replaceFirst("file:///", "")))
  }

  after {
    println("Done")
  }

  test("hdfs-lab test") {
    JavaMain.main(Array())
    val file = new File(outputPath.replaceFirst("file:///", ""))
    assertThat(file).exists()
  }
}