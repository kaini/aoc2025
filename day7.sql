-- Parse the input
--------------------------------------
create materialized view day7 as
select input, line_no, col_no, c
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') with ordinality x(line, line_no)
cross join lateral regexp_split_to_table(line, '') with ordinality y(c, col_no)
where i.day = 7
;

create index day7_a on day7(c);
create index day7_b on day7(input, c, line_no, col_no);
create index day7_c on day7(input, c, col_no, line_no);

-- Calculate the solution for part 1
--------------------------------------

-- A beam is split if the splitter has
-- 1) a splitter (or the start) above it one to the left or one to the right,
-- 2) without another splitter between them, and
-- 3) the splitter above itself is reached (or the start).
with recursive r as (
	select *
	from day7
	where c = 'S'
	union
	select i.*
	from r r
	join day7 i on i.input = r.input and i.c = '^' and ((r.c = '^' and (i.col_no = r.col_no - 1 or i.col_no = r.col_no + 1)) or (r.c = 'S' and i.col_no = r.col_no))
	where i.line_no > r.line_no
	and not exists (
		select *
		from day7 ii
		where ii.c = '^'
		and ii.input = i.input
		and ii.col_no = i.col_no
		and ii.line_no between r.line_no + 1 and i.line_no - 1
	)
)
select input, count(*)
from r
where c = '^'
group by input
;

-- Calculate the solution for part 2
--------------------------------------
with recursive r as (
	select *, 1::bigint ways
	from day7
	where c = 'S'
	union all
	select t.input, t.line_no, t.col_no, t.c, sum(t.ways)::bigint
	from (
		select i.*, r.ways
		from r r
		join day7 i on i.input = r.input and i.c = '^' and ((r.c = '^' and (i.col_no = r.col_no - 1 or i.col_no = r.col_no + 1)) or (r.c = 'S' and i.col_no = r.col_no))
		where i.line_no > r.line_no
		and not exists (
			select *
			from day7 ii
			where ii.c = '^'
			and ii.input = i.input
			and ii.col_no = i.col_no
			and ii.line_no between r.line_no + 1 and i.line_no - 1
		)
	) t
	group by t.input, t.line_no, t.col_no, t.c
)
select input, sum(ways)
from r
group by input
;



