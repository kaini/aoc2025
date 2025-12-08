-- Parse the input
--------------------------------------
create materialized view day8 as
select input, split_part(line, ',', 1)::bigint x, split_part(line, ',', 2)::bigint y, split_part(line, ',', 3)::bigint z
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') line
where i.day = 8
;

create index day8_a on day8 (input, x, y, z);

-- Calculate the solution for part 1
--------------------------------------
with recursive connection as (
	select t.input, t.ax, t.ay, t.az, t.bx, t.by, t.bz
	from (
		select a.input, a.x ax, a.y ay, a.z az, b.x bx, b.y by, b.z bz, rank() over (partition by a.input order by (a.x-b.x)*(a.x-b.x) + (a.y-b.y)*(a.y-b.y) + (a.z-b.z)*(a.z-b.z) asc) rank
		from day8 a
		join day8 b on a.input = b.input and (a.x, a.y, a.z) < (b.x, b.y, b.z)
	) t
	where t.rank <= (case t.input when 1 then 10 else 1000 end)
),
connection_bidi as (
	select input, ax, ay, az, bx, by, bz
	from connection
	union all
	select input, bx, by, bz, ax, ay, az
	from connection
),
graph as (
	select
		*,
		x||'/'||y||'/'||z graph  -- use the start node as unique graph id
	from day8
	union
	select r.input, c.bx, c.by, c.bz, r.graph
	from graph r
	join connection_bidi c on c.input = r.input and c.ax = r.x and c.ay = r.y and c.az = r.z
),
graph_unique as (
	select distinct input, array_agg((x, y, z) order by x, y, z) points
	from graph
	group by input, graph
)
select product(g.size)
from (
	select
		input,
		array_length(points, 1) size,
		row_number() over (partition by input order by array_length(points, 1) desc) i
	from graph_unique
) g
where g.i <= 3
group by g.input
;

-- Calculate the solution for part 2
--------------------------------------
with connection as (
	select a.input, a.x ax, a.y ay, a.z az, b.x bx, b.y by, b.z bz, row_number() over (partition by a.input order by (a.x-b.x)*(a.x-b.x) + (a.y-b.y)*(a.y-b.y) + (a.z-b.z)*(a.z-b.z) asc) i
	from day8 a
	join day8 b on a.input = b.input and (a.x, a.y, a.z) < (b.x, b.y, b.z)
),
result as (
	select
		input,
		i,
		ax,
		bx,
		(string_agg(ax||'/'||ay||'/'||az||'=>1, '||bx||'/'||by||'/'||bz||'=>1', ',') over (partition by input order by i rows between unbounded preceding and current row))::hstore graph
	from connection
),
input as (
	select input, count(*) points
	from day8
	group by input
)
select i.input, r.ax * r.bx
from input i
cross join lateral (
	select *
	from result r
	where r.input = i.input
	and (select count(*) from each(r.graph)) = i.points
	order by r.input, r.i
	limit 1
) r
;



