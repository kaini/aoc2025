-- Parse the input
--------------------------------------
create materialized view day2_input as
select input, cast(split_part(range, '-', 1) as bigint) start, cast(split_part(range, '-', 2) as bigint) end
from input i
cross join lateral string_to_table(i.value, ',') x(range)
where i.day = 2
;

-- Calculate the solution for part 1
--------------------------------------
with recursive explode(input, start, "end", n, str) as (
	select i.input, i.start, i.end, i.start, cast(i.start as text)
	from day2_input i
	union all
	select explode.input, explode.start, explode.end, explode.n + 1, cast(explode.n + 1 as text)
	from explode
	where explode.n < explode.end
)
select input, sum(n)
from explode
where str ~ '^(.+)\1$'
group by input
;


-- Calculate the solution for part 2
--------------------------------------
with recursive explode(input, start, "end", n, str) as (
	select i.input, i.start, i.end, i.start, cast(i.start as text)
	from day2_input i
	union all
	select explode.input, explode.start, explode.end, explode.n + 1, cast(explode.n + 1 as text)
	from explode
	where explode.n < explode.end
)
select input, sum(n)
from explode
where str ~ '^(.+)\1+$'
group by input
;


