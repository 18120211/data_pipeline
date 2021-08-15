drop database if exists customer_order_raw;

create database customer_order_raw;

use customer_order_raw;

create table row_data(
	row_id bigint,
    order_id nvarchar(50),
    order_date datetime,
    ship_date datetime,
    ship_mode nvarchar(50),
    customer_id nvarchar(50),
    customer_name nvarchar(200),
    segment nvarchar(50),
    country nvarchar(50),
    city nvarchar(50),
    state nvarchar(50),
    postal_code int,
    region nvarchar(50),
    product_id nvarchar(50),
    category nvarchar(50),
    sub_category nvarchar(50),
    product_name nvarchar(200),
    sales float,
    quantity int,
    discount float,
    profit float,
    constraint primary key(row_id)
);












