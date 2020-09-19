package fr.aretex.irex.hdfs.fonctions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Builder
@AllArgsConstructor
public class ReadAllFiles implements Supplier<List<Path>> {
    private final Path inputPath;
    protected final FileSystem hdfs;

    @Override
    public List<Path> get() {
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
                List<Path> listPaths = new ArrayList<>();
                while(fileIterator.hasNext()){
                    LocatedFileStatus locatedFileStatus = fileIterator.next();
                    log.info("getPath={}>",
                            locatedFileStatus.getPath().getName().toString()
                    );
                    listPaths.add(locatedFileStatus.getPath());
                }
                return listPaths;
            }

            return null;
        } catch (Exception ex) {
            log.info("could not read files located at inputPath={} because :",
                    inputPath, ex
            );

            return null;
        }
    }
}
