drop database if exists metadata;

create database metadata;

use metadata;

create table logger(
	id int auto_increment,
    date_modified datetime default current_timestamp,
    detail nvarchar(100),
    number_row_affected int,
    constraint primary key(id)
);