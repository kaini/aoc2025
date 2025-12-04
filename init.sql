create table input (
	day int,
	input int,
	value text,
	primary key (day, input)
);

create extension hstore;

-- Do not forget to set random_page_cost to 1 if you have an SSD!

