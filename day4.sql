-- Parse the input
--------------------------------------
create materialized view day4_input as
select input, x, y, c
from input i
cross join lateral string_to_table(i.value, E'\n') with ordinality l(line, y)
cross join lateral regexp_split_to_table(l.line, '') with ordinality cell(c, x)
where i.day = 4
and c not in (E'\n', E'\r', E' ')
;

create index day4_a on day4_input (input, x, y, c);
create index day4_b on day4_input (input, c);


-- Calculate the solution for part 1
--------------------------------------
select i.input, count(*)
from day4_input i
left join day4_input a on a.input = i.input and a.x = i.x + 1 and a.y = i.y + 1 and a.c = '@'
left join day4_input b on b.input = i.input and b.x = i.x + 1 and b.y = i.y     and b.c = '@'
left join day4_input c on c.input = i.input and c.x = i.x + 1 and c.y = i.y - 1 and c.c = '@'
left join day4_input d on d.input = i.input and d.x = i.x     and d.y = i.y + 1 and d.c = '@'
left join day4_input e on e.input = i.input and e.x = i.x     and e.y = i.y - 1 and e.c = '@'
left join day4_input f on f.input = i.input and f.x = i.x - 1 and f.y = i.y + 1 and f.c = '@'
left join day4_input g on g.input = i.input and g.x = i.x - 1 and g.y = i.y     and g.c = '@'
left join day4_input h on h.input = i.input and h.x = i.x - 1 and h.y = i.y - 1 and h.c = '@'
where i.c = '@'
and ((a.c is not null)::int + (b.c is not null)::int + (c.c is not null)::int + (d.c is not null)::int + (e.c is not null)::int + (f.c is not null)::int + (g.c is not null)::int + (h.c is not null)::int) < 4
group by i.input
;

-- Calculate the solution for part 2
--------------------------------------

with recursive result(input, taken) as (
	select distinct i.input, ''::hstore
	from day4_input i
	union
	select
		r.input,
		r.taken || coalesce((
			select string_agg(i.x || '-' || i.y || '=>1', ',')::hstore
			from day4_input i
			left join day4_input a on a.input = i.input and a.x = i.x + 1 and a.y = i.y + 1 and a.c = '@' and not (r.taken ? (a.x||'-'||a.y))
			left join day4_input b on b.input = i.input and b.x = i.x + 1 and b.y = i.y     and b.c = '@' and not (r.taken ? (b.x||'-'||b.y))
			left join day4_input c on c.input = i.input and c.x = i.x + 1 and c.y = i.y - 1 and c.c = '@' and not (r.taken ? (c.x||'-'||c.y))
			left join day4_input d on d.input = i.input and d.x = i.x     and d.y = i.y + 1 and d.c = '@' and not (r.taken ? (d.x||'-'||d.y))
			left join day4_input e on e.input = i.input and e.x = i.x     and e.y = i.y - 1 and e.c = '@' and not (r.taken ? (e.x||'-'||e.y))
			left join day4_input f on f.input = i.input and f.x = i.x - 1 and f.y = i.y + 1 and f.c = '@' and not (r.taken ? (f.x||'-'||f.y))
			left join day4_input g on g.input = i.input and g.x = i.x - 1 and g.y = i.y     and g.c = '@' and not (r.taken ? (g.x||'-'||g.y))
			left join day4_input h on h.input = i.input and h.x = i.x - 1 and h.y = i.y - 1 and h.c = '@' and not (r.taken ? (h.x||'-'||h.y))
			where i.input = r.input
			and i.c = '@'
			and not (r.taken ? (i.x||'-'||i.y))
			and ((a.c is not null)::int + (b.c is not null)::int + (c.c is not null)::int + (d.c is not null)::int + (e.c is not null)::int + (f.c is not null)::int + (g.c is not null)::int + (h.c is not null)::int) < 4
		), ''::hstore)
	from result r
)
select input, max((select count(*) from each(taken)))
from result
group by input
;



