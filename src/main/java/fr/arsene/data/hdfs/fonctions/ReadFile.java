package fr.aretex.irex.hdfs.fonctions;

import lombok.AllArgsConstructor;
import lombok.Builder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.hadoop.fs.FileSystem.get;


@Slf4j
@RequiredArgsConstructor
@Builder
@AllArgsConstructor
public class ReadFile implements Supplier<Optional<RemoteIterator<LocatedFileStatus>>> {

    private final Path inputPath;
    protected final FileSystem hdfs;

    @Override
    public Optional<RemoteIterator<LocatedFileStatus>> get() {

        boolean isFileExists = false;
        if (hdfs != null) {
            try {
                isFileExists = hdfs.exists(new Path(String.valueOf(inputPath)));
            } catch (IOException ioe) {
                log.warn("Une exception a été rencontrée");
            }
        }

        try {
            if (isFileExists && hdfs != null) {
                RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true);
                return Optional.of(fileIterator);
            }

            return Optional.empty();
        } catch (Exception ex) {
            log.info("could not read files located at inputPath={} faute de :",
                    inputPath, ex
            );

            return Optional.empty();
        }

  /*  public static void readerPath(FileSystem hdfs,Path inputPath ) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true);
        LocatedFileStatus locatedFileStatus = fileIterator.next();

            FSDataInputStream instream = hdfs.open(locatedFileStatus.getPath());
    }*/

  /*  private static Supplier<Optional<RemoteIterator<LocatedFileStatus>>> linesSupplier(FileSystem hdfs,Path inputPath ) throws IOException {
        return () -> {
            try {
                return Optional.of(
                        RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(inputPath, true));

            } catch (IOException ioe) {
               ioe.printStackTrace();
            }
            return Optional.empty();
        };
    } */

    }
}


