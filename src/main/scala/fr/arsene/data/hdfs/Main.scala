package fr.aretex.irex.hdfs

import java.io.StringWriter
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.slf4j.{Logger, LoggerFactory}

object Main {
  private val logger: Logger = LoggerFactory.getLogger(Main.getClass)
  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load("app.properties")

    val appName = config.getString("app.name")
    val appID = formatter.format(LocalDateTime.now()) + "_" + math.floor(math.random * 10E4).toLong.toString

    val hdfs = FileSystem.get(new Configuration())

    logger.info(s"hdfsConf = <${hdfs.getScheme}, $appName, $appID>")

    /**DECLATION DES VARIABLES**/
    val inputPath = new Path(config.getString("app.data.input"))
    val outputPath = new Path(config.getString("app.data.output"))

    /**DATA PROCESSING**/
    logger.info(s"Listing files from inputPath=$inputPath ...")
    val fileIterator = hdfs.listFiles(inputPath, true)
    while(fileIterator.hasNext){
      val locatedFileStatus = fileIterator.next()
      logger.info(s"fileInfo=<${locatedFileStatus.getPath.getName}, ${locatedFileStatus.getLen}, ${locatedFileStatus.getBlockSize}, ${locatedFileStatus.getPath.toString}>")
      val instream = hdfs.open(locatedFileStatus.getPath)
      val writer = new StringWriter()
      IOUtils.copy(instream, writer, "UTF-8")

      val lines = writer.toString
        .split("\n")
        .filter(l => !l.startsWith("\"")) // remove header
      val wordCount = lines.flatMap(_.split(";"))
        .filter(x => !NumberUtils.isNumber(x))
        .map(x => (x, 1))
        .groupBy(x => x._1).map(x => (x._1, x._2.map(_._2).sum))

      logger.info(s"wordCount=<${wordCount}>")
      wordCount.foreach(x => logger.info(s"$x"))

      val outlines = wordCount.map(_.toString).reduce((x,y)=> x + "\n" + y)
      val outstream = hdfs.create(outputPath.suffix(s"/wordcount/${locatedFileStatus.getPath.getName}"))
      IOUtils.write(outlines, outstream, "UTF-8")
      outstream.close()
      instream.close()

    }

    /**DATA SAVING**/
    logger.info(s"Copying files from inputPath=$inputPath to outputPath=$outputPath ...")
    FileUtil.copy(
      FileSystem.get(inputPath.toUri, hdfs.getConf), inputPath,
      FileSystem.get(outputPath.toUri, hdfs.getConf), outputPath,
      false, hdfs.getConf
    )



  }

}
