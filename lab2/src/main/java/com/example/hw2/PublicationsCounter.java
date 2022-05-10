package com.example.hw2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;

@Slf4j
public class PublicationsCounter {

    private static final int ENTRANCE = 0;
    private static final int EXIT = 1;

    public static Dataset<Row> countPublications(
            Dataset<Row> publicationsDataset,
            Dataset<Row> attendanceDataset
    ) {
        // формируем посещаемость по каждому году,
        // суммируя метки времени для каждого события в году для пользователя
        attendanceDataset = attendanceDataset.withColumn("timestamp", col("timestamp").cast("int"))
                .groupBy(
                        col("id"),
                        col("year"),
                        col("event_type"),
                        col("university")
                ).sum("timestamp")
                .select(
                        col("id"),
                        col("year"),
                        col("event_type"),
                        col("university"),
                        col("sum(timestamp)").as("timestamp")
                );

        // фильтр по событиям входа
        Dataset<Row> filteredEntrances = attendanceDataset.filter(
                col("event_type").equalTo(ENTRANCE)
        );
        // фильтр по событиям выхода
        Dataset<Row> filteredExits = attendanceDataset.filter(
                col("event_type").equalTo(EXIT)
        ).select(
                col("id").as("id_right"),
                col("event_type").as("event_type_right"),
                col("timestamp").as("timestamp_exit"),
                col("year").as("year_right")
        );

        // слияние с подсчетом разности времени нахождения
        // получаем суммарное нахождение пользователя в университете для конкретного года
        Dataset<Row> joinedDf = filteredEntrances.join(
                filteredExits,
                filteredEntrances.col("id").equalTo(filteredExits.col("id_right"))
                        .and(filteredEntrances.col("year").equalTo(filteredExits.col("year_right")))
        ).select(
                col("university"),
                col("id"),
                col("year"),
                abs(col("timestamp_exit").minus(col("timestamp"))).as("time_by_year"),
                col("year")
        );
        joinedDf.show();

        // подсчет публикаций по году и пользователю
        Dataset<Row> publicationsCountByYear = publicationsDataset
                .groupBy(
                        col("id"),
                        col("year")
                ).count()
                .select(
                        col("id").as("id_right"),
                        col("year").as("year_right"),
                        col("count").as("publications_num")
                );

        // слияние двух датасетов
        return joinedDf.join(
                publicationsCountByYear,
                joinedDf.col("id").equalTo(publicationsCountByYear.col("id_right")).and(
                        joinedDf.col("year").equalTo(publicationsCountByYear.col("year_right"))
                )
        ).select(
                col("university"),
                col("id"),
                col("year"),
                col("time_by_year"),
                col("publications_num")
        );
    }
}
