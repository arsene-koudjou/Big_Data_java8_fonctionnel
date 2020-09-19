package fr.aretex.irex.hdfs.fonctions;

import com.ctc.wstx.io.MergedStream;
import com.sun.scenario.effect.Merge;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.tree.MergeCombiner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.Merger;
import scala.Tuple2;


import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
@Builder

public class MergeAllFiles implements Function<List<Path>, Path > {
    private final Path outputPath1;
    protected FileSystem hdfs;
    private LocatedFileStatus locatedFileStatus;

    @Override
    public Path apply(List<Path> paths) {
        for (Path p: paths
             ) {
            FSDataInputStream instream = null;
            try {
                instream = hdfs.open(p);
            } catch (IOException e) {
                e.printStackTrace();
            }
            StringWriter writer = new StringWriter();
            try {
                IOUtils.copy(instream, writer, "UTF-8");

            } catch (IOException e) {
                e.printStackTrace();
            }

            Stream<String> lines = Arrays.stream(writer.toString().split("\n"))
                    .filter(l -> !l.startsWith("\"")); // remove header

            Map<String, Integer> wordCount = lines.flatMap(l -> Arrays.stream(l.split(";")))
                    .filter(x -> !NumberUtils.isDigits(x))
                    .map(x -> new Tuple2<>(x, 1))
                    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2, Integer::sum));

            log.info("wordCount");
            String nom = "je m appelle Arsene Koudjou Takoutcha ";
            wordCount.forEach((k, v) -> log.info("({} -> {})", k, v));
            String outlines = wordCount.entrySet().stream()
                    .map(e -> String.format("%s,%d", e.getKey(), e.getValue()))
                    .collect(Collectors.joining("\n"));
            FSDataOutputStream outstream = null;
            try {
                outstream = hdfs.create(outputPath1.suffix(String.format("/wordcount/%s", locatedFileStatus.getPath().getName())));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return outputPath1;
    }
}
