drop database if exists customer_order;

create database customer_order;

use customer_order;

create table dim_date (
	id int,
    date date,
    constraint primary key(id)
);

create table dim_customer(
	id int auto_increment,
    customer_id nvarchar(50),
    customer_name nvarchar(200),
    segment nvarchar(50),
    country nvarchar(50),
    city nvarchar(50),
    state nvarchar(50),
    postal_code int,
    region nvarchar(50),
    status nvarchar(50),
    constraint primary key(id),
    constraint check (region in ('South', 'West', 'East', 'North', 'Central')),
    constraint check (status in('expired', 'active'))
);

create table dim_product(
	id int auto_increment,
    product_id nvarchar(50),
    product_name nvarchar(200),
    category nvarchar(50),
    sub_category nvarchar(50),
    status nvarchar(50),
    constraint primary key(id),
    constraint check (status in('expired', 'active'))
);

create table fact_order(
	row_id bigint,
    order_id nvarchar(50),
    order_date int,
    ship_date int,
    ship_mode nvarchar(50),
	customer_id int,
    product_id int,
    sales float,
    quantity int,
    discount float,
    profit float,
    constraint primary key (row_id),
	constraint foreign key (order_date) references dim_date(id),
    constraint foreign key (ship_date) references dim_date(id),
    constraint foreign key (customer_id) references dim_customer(id),
    constraint foreign key (product_id) references dim_product(id)
);
