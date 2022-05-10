package com.example.hw2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Переименование колонок таблицы, тк предполагается считывание
 * файла без header-а
 */
public class DatasetColumnsRenamer {

    private static final Map<String, String> attendanceColumnNames = Stream.of(new String[][]{
            {"_c0", "university"},
            {"_c1", "id"},
            {"_c2", "timestamp"},
            {"_c3", "event_type"},
            {"_c4", "year"},
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    private static final Map<String, String> publicationsColumnNames = Stream.of(new String[][]{
            {"_c0", "university"},
            {"_c1", "id"},
            {"_c2", "publication"},
            {"_c3", "year"},
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    public static Dataset<Row> renameAttendanceColumns(Dataset<Row> df) {
        for (String key : attendanceColumnNames.keySet()) {
            df = df.withColumnRenamed(key, attendanceColumnNames.get(key));
        }
        return df;
    }

    public static Dataset<Row> renamePublicationColumns(Dataset<Row> df) {
        for (String key : publicationsColumnNames.keySet()) {
            df = df.withColumnRenamed(key, publicationsColumnNames.get(key));
        }
        return df;
    }
}