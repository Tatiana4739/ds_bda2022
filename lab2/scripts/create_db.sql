drop database if exists hw2;
drop role if exists hw2;

create user hw2 createdb password 'some_password';

create database hw2 owner hw2;
\connect hw2

DROP TABLE if exists attendance;
CREATE TABLE attendance
(
  university varchar NOT NULL,
  id varchar NOT NULL,
  timestamp varchar NOT NULL,
  event_type varchar NOT NULL,
  year varchar NOT NULL
);
COPY attendance FROM '/home/qqq/IdeaProjects/lab2/scripts/attendance.csv' DELIMITER ',' CSV HEADER;
ALTER TABLE attendance OWNER TO hw2;

DROP TABLE if exists publications;
CREATE TABLE publications
(
  university varchar NOT NULL,
  id varchar NOT NULL,
  publication varchar NOT NULL,
  year varchar NOT NULL
);
COPY publications FROM '/home/qqq/IdeaProjects/lab2/scripts/publications.csv' DELIMITER ',' CSV HEADER;
ALTER TABLE publications OWNER TO hw2;
