-- Parse the input
--------------------------------------

create materialized view day1_input as
select i.input, x.index, substr(x.line, 1, 1) direction, cast(substr(x.line, 2) as int) amount
from input i
cross join lateral string_to_table(i.value, E'\n') with ordinality x(line, index)
where i."day" = 1
;

create index day1_input_a on day1_input (input, index);

-- Calculate the solution for part 1
--------------------------------------

select i.input, count(*) solution
from (
	select
		i.input,
		(50 + sum((case i.direction when 'L' then -1 else 1 end) * i.amount) over (partition by i.input order by i.index asc)) % 100 value
	from day1_input i
) i
where i.value = 0
group by i.input
;

-- Calculate the solution for part 2
--------------------------------------

with recursive vs(input, index, amount, value, cnt) as (
	select i.input, 0::bigint, 0::bigint, 50::numeric, 0::numeric
	from day1_input i
	where i.index = 1
	union all
	select
		i.input,
		i.index,
		i.amount,
		pmod(vs.value + (case i.direction when 'L' then -1 else 1 end) * i.amount, 100),
		floor(abs((vs.value + (case i.direction when 'L' then -1 else 1 end) * i.amount) / 100)) + case when vs.value + (case i.direction when 'L' then -1 else 1 end) * i.amount <= 0 and vs.value <> 0 then 1 else 0 end
	from vs
	join day1_input i on i.input = vs.input and i.index = vs.index + 1
)
select input, sum(vs.cnt)
from vs
group by input
;
