create table input (
	day int,
	input int,
	value text,
	primary key (day, input)
);

create extension hstore;

-- "real" modulo function that always returns positive values - never negative ones like %.
create or replace function pmod(a numeric, b numeric)
returns numeric
immutable leakproof strict parallel safe as $$
declare
	x numeric;
begin
	if a is null or b is null then
		return null;
	end if;

	x := a % b;
	if x < 0 then
		x := x + b;
	end if;
	return x;
end;
$$ language plpgsql;

-- product aggregate function
create or replace aggregate product(bigint) (
	sfunc = int8mul,
	stype = bigint,
	initcond = '1',
	parallel = safe
);

-- Do not forget to set random_page_cost to 1 if you have an SSD!

