package fr.aretex.irex.hdfs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


public class MainIntegrationTest {
    private static Logger logger = LoggerFactory.getLogger(MainIntegrationTest.class);
    private static Config config = ConfigFactory.load("app.properties");
    private static Configuration hadoopConf = new Configuration();

    private static String relativeLocalInputPath = config.getString("app.data.input");
    private static String relativeLocalOutputPath = config.getString("app.data.output");

    private static Path inputPath = new Path(relativeLocalInputPath);
    private static Path outputPath = new Path(relativeLocalOutputPath);

    private static FileSystem fs;
    private static FileSystem hdfs;

    @BeforeClass
    public static void setUp() throws IOException {
        logger.info("init hdfs");
        fs = FileSystem.getLocal(hadoopConf);
        logger.info("fs={}", fs.getScheme());
        hdfs = FileSystem.get(hadoopConf);
        logger.info("hdfs={}", hdfs.getScheme());
        clean();
        hdfs.mkdirs(inputPath);
        hdfs.copyFromLocalFile(inputPath, inputPath);
        assertThat(hdfs.exists(inputPath)).isTrue();
        assertThat(hdfs.listFiles(inputPath, true).hasNext()).isTrue();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        clean();
    }

    private static void clean() throws IOException {
        if(fs != null){
            fs.delete(outputPath, true);
        }
        if(hdfs != null){
            hdfs.delete(inputPath, true);
            hdfs.delete(outputPath, true);
        }
    }

    @Test
    public void checkOutputDir() throws IOException {
        JavaMain.main(new String[0]);
        assertThat(hdfs.exists(outputPath)).isTrue();
    }
}
