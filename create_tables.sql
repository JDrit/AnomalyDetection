drop table if exists views;
drop table if exists anomalies;
drop table if exists trending;

create table views (
    page_title varchar(512),
    timestamp timestamp,
    views int,
    primary key (page_title, timestamp)
);

create table anomalies (
    page_title varchar(512),
    start_timestamp timestamp,
    end_timestamp timestamp,
    highest_value int,
    primary key (page_title, start_timestamp, end_timestamp)
);

create table trending (
    page_title varchar(512),
    kdistance int,
    primary key (page_title)
);
