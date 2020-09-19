package fr.aretex.irex.hdfs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.aretex.irex.hdfs.fonctions.DataConsumer;
import fr.aretex.irex.hdfs.fonctions.MergeAllFiles;
import fr.aretex.irex.hdfs.fonctions.ReadAllFiles;
import fr.aretex.irex.hdfs.fonctions.ReadFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class JavaMain {
  private static Logger logger = LoggerFactory.getLogger(JavaMain.class);
  private static DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  public static void main(String[] args) throws IOException {

    Config config = ConfigFactory.load("app.properties");

    String appName = config.getString("app.name");
    String appID = formatter.format(LocalDateTime.now()) + "_" + new Random().nextLong();

    FileSystem hdfs = FileSystem.get(new Configuration());

    logger.info("Welcome Sir ");
    logger.info("my name is Arsene Koudjou Takoutcha ");
    logger.info("hdfsConf = <{}, {}, {}>", hdfs.getScheme(), appName, appID);

    /**DECLATION DES CHEMINS**/
    Path inputPath = new Path(config.getString("app.data.input"));
    Path outputPath1 = new Path(config.getString("app.data.output1"));
    Path outputPath = new Path(config.getString("app.data.output"));

    /**TRAITEMENT DES DONNEES**/

    logger.info("Listing files from inputPath={} ...", inputPath);
      /**Supplier ReadAllFiles**/
      logger.info("The supplier named ReadFile will read files ...");
     ReadAllFiles reader = new ReadAllFiles(inputPath,hdfs);
      List<Path> readerPath = reader.get();

      /**Fonction MergeAllFiles**/
      logger.info("The function  named MergeAllFiles will merge files into a single file ...");
      MergeAllFiles m = new MergeAllFiles(outputPath1);
      Path ph = m.apply(readerPath);

      /**Consumer DataConsumer**/
      logger.info("The consumer named DataConsumer will save  data ...");
      DataConsumer d =  new DataConsumer(outputPath);
      d.accept(ph);


    logger.info("Copying files from inputPath={} to outputPath={} ...", inputPath, outputPath);
    FileUtil.copy(
            FileSystem.get(inputPath.toUri(), hdfs.getConf()),
            inputPath,
            FileSystem.get(outputPath.toUri(), hdfs.getConf()),
            outputPath,
            false, hdfs.getConf()
    );
    logger.info("End of Big Data processing");

  }

}
