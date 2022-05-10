package com.example.hw2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublicationsCounterTest {

    private final SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("Java Spark SQL hw2 Tests")
            .getOrCreate();
    
    private Dataset<Row> testAttendanceDataset;
    private Dataset<Row> testPublicationsDataset;

    /**
     * Сравнение результата, полученных из входных данных файлов в src/test/resources
     * и содержимого src/test/resources/result1.csv
     */
    @Test
    public void testSimpleDataset() {
        readDatasetsFromResources();

        Dataset<Row> result = PublicationsCounter.countPublications(testPublicationsDataset, testAttendanceDataset);

        Dataset<Row> trueResult = ss.read()
                .format("csv")
                .option("header", true)
                .load("src/test/resources/result1.csv");

        compareTwoDatasets(trueResult, result);
    }

    /**
     * Тест на пустые входные данные
     */
    @Test
    public void testEmptyDataSet() {
        prepareEmptyDatasets();

        Dataset<Row> result = PublicationsCounter.countPublications(testPublicationsDataset, testAttendanceDataset);

        StructType outputStructType = new StructType()
                .add(new StructField("university", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("year", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("time_by_year", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("publications_num", DataTypes.StringType, true, Metadata.empty()));

        Dataset<Row> trueEmptyDataset = ss.read().schema(outputStructType).csv(ss.emptyDataset(Encoders.STRING()));

        compareTwoDatasets(trueEmptyDataset, result);
    }

    private void prepareEmptyDatasets() {
        StructType attendanceStructType = new StructType()
                .add(new StructField("university", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("event_type", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("year", DataTypes.StringType, true, Metadata.empty()));

        testAttendanceDataset = ss.read().schema(attendanceStructType).csv(ss.emptyDataset(Encoders.STRING()));

        StructType publicationsStructType = new StructType()
                .add(new StructField("university", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("id", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("publication", DataTypes.StringType, true, Metadata.empty()))
                .add(new StructField("year", DataTypes.StringType, true, Metadata.empty()));

        testPublicationsDataset = ss.read().schema(publicationsStructType).csv(ss.emptyDataset(Encoders.STRING()));
    }

    private void readDatasetsFromResources() {
        testAttendanceDataset = ss.read()
                .option("header", true)
                .csv("src/test/resources/attendance1.csv");
        testPublicationsDataset = ss.read()
                .option("header", true)
                .csv("src/test/resources/publications1.csv");
    }

    private void compareTwoDatasets(Dataset<Row> trueDataset, Dataset<Row> resultDataset) {
        assertEquals(trueDataset.select("university").collectAsList(), resultDataset.select("university").collectAsList());
        assertEquals(trueDataset.select("id").collectAsList(), resultDataset.select("id").collectAsList());
        assertEquals(trueDataset.select("year").collectAsList(), resultDataset.select("year").collectAsList());
        compareTwoLists(trueDataset.select("time_by_year").collectAsList(), resultDataset.select("time_by_year").collectAsList());
        compareTwoLists(trueDataset.select("publications_num").collectAsList(), resultDataset.select("publications_num").collectAsList());
    }

    private <T> void compareTwoLists(List<T> expectedList, List<T> actualList) {
        List<String> expectedCount = expectedList.stream().map(T::toString).collect(
                Collectors.toList()
        );
        List<String> actualCount = actualList.stream().map(T::toString).collect(
                Collectors.toList()
        );
        assertEquals(expectedCount, actualCount);
    }
}
