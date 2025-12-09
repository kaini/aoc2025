-- Parse the input
--------------------------------------
create materialized view day9 as
select input, n, split_part(line, ',', 1)::bigint x, split_part(line, ',', 2)::bigint y
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') with ordinality l(line, n)
where i.day = 9
;

create index day9_a on day9 (input, x, y);
create index day9_b on day9 (input, n);

-- Calculate the solution for part 1
--------------------------------------
select a.input, max((abs(a.x - b.x) + 1) * (abs(a.y - b.y) + 1))
from day9 a
join day9 b on b.input = a.input and (a.x, a.y) < (b.x, b.y)
group by a.input
;

-- Calculate the solution for part 2
--------------------------------------
with line as (
	select a.input, a.x ax, a.y ay, b.x bx, b.y by
	from day9 a
	join day9 b on b.input = a.input and b.n = (case a.n when (select count(*) from day9 where input = a.input) then 1 else a.n + 1 end)
)
select a.input, count(*), max((abs(a.x - b.x) + 1) * (abs(a.y - b.y) + 1))
from day9 a
join day9 b on b.input = a.input and (a.x, a.y) < (b.x, b.y)
where not exists (
	select *
	from line l
	where l.input = a.input
	and (
		(l.ay = l.by and least(a.x, b.x) between least(l.ax, l.bx) and greatest(l.ax, l.bx) - 1 and l.ay between (least(a.y, b.y) + 1) and (greatest(a.y, b.y) - 1))
		or (l.ay = l.by and greatest(a.x, b.x) between least(l.ax, l.bx) + 1 and greatest(l.ax, l.bx) and l.ay between (least(a.y, b.y) + 1) and (greatest(a.y, b.y) - 1))
		or (l.ax = l.bx and least(a.y, b.y) between least(l.ay, l.by) and greatest(l.ay, l.by) - 1 and l.ax between (least(a.x, b.x) + 1) and (greatest(a.x, b.x) - 1))
		or (l.ax = l.bx and greatest(a.y, b.y) between least(l.ay, l.by) + 1 and greatest(l.ay, l.by) and l.ax between (least(a.x, b.x) + 1) and (greatest(a.x, b.x) - 1))
	)
)
group by a.input
;


-- I used this query to check that there are no lines that are touching, as this would make the problem much harder.
with line as (
	select a.input, a.x ax, a.y ay, b.x bx, b.y by
	from day9 a
	join day9 b on b.input = a.input and b.n = (case a.n when (select count(*) from day9 where input = a.input) then 1 else a.n + 1 end)
)
select *
from line a
join line b on a.input = b.input and (a.ax, a.ay, a.bx, a.by) < (b.ax, b.ay, b.bx, b.by)
where (a.ax = a.bx and b.ax = b.bx and b.ax in (a.ax - 1, a.ax, a.ax + 1))
or (a.ay = a.by and b.ay = b.by and b.ay in (a.ay - 1, a.ay, a.ay + 1))
;


