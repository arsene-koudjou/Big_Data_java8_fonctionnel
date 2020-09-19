package fr.aretex.irex.hdfs.fonctions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
@Builder
public class DataConsumer implements Consumer<Path> {
    private final Path outputPath;
    protected FileSystem hdfs;
    private LocatedFileStatus locatedFileStatus;
    private String outlines;

    @Override
    public void accept(Path path) {
        String nom = "je m appelle Arsene Koudjou Takoutcha ";
            FSDataOutputStream outstream = null;
            try {
                outstream = hdfs.create(outputPath.suffix(String.format("/wordcount/%s", locatedFileStatus.getPath().getName())));
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                IOUtils.writeLines(Collections.singleton(nom), "", outstream, "UTF-8");
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                IOUtils.writeLines(Collections.singleton(outlines), "", outstream, "UTF-8");
            } catch (IOException e) {
                e.printStackTrace();
            }
            String footer = "le nombre de lignes est : " + outlines.length();
            try {
                IOUtils.writeLines(Collections.singleton(footer), "", outstream, "UTF-8");
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                outstream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
}
