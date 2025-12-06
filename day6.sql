-- Parse the input
--------------------------------------
create materialized view day6_num as
select input, line_no, num_no, cast(num as bigint)
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') with ordinality x(line, line_no)
cross join lateral regexp_split_to_table(trim(line), ' +') with ordinality y(num, num_no)
where i.day = 6
and num ~ '[0-9]+'
;

create materialized view day6_op as
select input, op_no, op
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') line
cross join lateral regexp_split_to_table(trim(line), ' +') with ordinality y(op, op_no)
where i.day = 6
and op !~ '[0-9]+'
;

create materialized view day6_raw as
with raw as (
	select i.input, line_no, col_no, col, max(line_no) over (partition by i.input) max_line_no
	from input i
	cross join lateral regexp_split_to_table(i.value, '\r?\n') with ordinality l(line, line_no)
	cross join lateral regexp_split_to_table(line, '') with ordinality c(col, col_no)
	where i.day = 6
)
select r.input, r.line_no, r.col_no, r.col
from raw r
where r.line_no <> r.max_line_no
;

create index day6_a on day6_op(input, op_no, op);

create index day6_b on day6_raw(input, col_no, line_no, col);


-- Calculate the solution for part 1
--------------------------------------
select n.input, sum(n.result)
from (
	select
		n.input,
		n.num_no,
		case (select o.op from day6_op o where o.input = n.input and o.op_no = n.num_no)
			when '+' then sum(n.num)
			when '*' then product(n.num)
			else null
		end::bigint result
	from day6_num n
	group by n.input, n.num_no
) n
group by n.input
;


-- Calculate the solution for part 2
--------------------------------------
with input as (
	select r.input, r.col_no, trim(string_agg(r.col, '' order by r.line_no asc)) n
	from day6_raw r
	group by r.input, r.col_no
),
input_groups as (
	select *, (select count(*) + 1 from input ii where ii.input = i.input and ii.col_no < i.col_no and ii.n = '') group_no
	from input i
	where i.n <> ''
)
select i.input, sum(i.result)
from (
	select
		i.input,
		i.group_no,
		case (select o.op from day6_op o where o.input = i.input and o.op_no = i.group_no)
			when '+' then sum(i.n::bigint)
			when '*' then product(i.n::bigint)
			else null
		end result
	from input_groups i
	group by i.input, i.group_no
) i
group by i.input
;



