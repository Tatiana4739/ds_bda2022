package com.example.hw2;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

@Slf4j
public class MainApplication {

    private static String hdfsPrefix = "hdfs://127.0.0.1:9000";
    private static final String attendanceFileName = "/part-m-00000";
    private static final String publicationsFileName = "/part-m-00000";

    public static void main(String[] args) {

        if (args.length < 3) {
            throw new RuntimeException("Usage: ./gradlew run --args=\"<attendance_dir> <publications_dir> <output_dir> <hdfs_prefix_opt>\"");
        }
        if (args.length == 4)
            hdfsPrefix = args[3];

        String attendanceDir = args[0];
        String publicationsDir = args[1];
        String outputDir = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL hw2")
                .config("spark.master", "local")
                .getOrCreate();

        try {
            FileSystem fileSystem = FileSystem.get(new URI(hdfsPrefix), spark.sparkContext().hadoopConfiguration());
            Path outputDirectory = new Path(hdfsPrefix + outputDir);
            if (fileSystem.exists(outputDirectory)) {
                log.info("=== Deleting output directory ===");
                fileSystem.delete(outputDirectory, true);
            }
        } catch (IOException exc) {
            log.error("Error listing files from hdfs!");
            exc.printStackTrace();
            return;
        } catch (URISyntaxException e) {
            log.error("Invalid hdfs path!");
            e.printStackTrace();
            return;
        }

        Path hdfsPublicationsPath = new Path(hdfsPrefix + publicationsDir + publicationsFileName);
        Dataset<Row> publicationsDataFrame = spark
                .read()
                .csv(hdfsPublicationsPath.toString());
        publicationsDataFrame = DatasetColumnsRenamer.renamePublicationColumns(publicationsDataFrame);

        Path hdfsAttendancePath = new Path(hdfsPrefix + attendanceDir + attendanceFileName);
        Dataset<Row> attendanceDataFrame = spark
                .read()
                .csv(hdfsAttendancePath.toString());
        attendanceDataFrame = DatasetColumnsRenamer.renameAttendanceColumns(attendanceDataFrame);

        log.info("========= COUNTING STARTED =========");
        Dataset<Row> resultDf = PublicationsCounter.countPublications(publicationsDataFrame, attendanceDataFrame);
        log.info("========= COUNTING ENDED =========");
        resultDf.show();

        resultDf.write()
                .option("header", true)
                .csv(hdfsPrefix + outputDir);
    }
}
