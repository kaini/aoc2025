-- Parse the input
--------------------------------------
create materialized view day5_range as
select i.input, cast((regexp_split_to_array(r.range, '-'))[1] as bigint) start, cast((regexp_split_to_array(r.range, '-'))[2] as bigint) end, r.id
from (
	select *
	from input i
	cross join lateral regexp_split_to_table(i.value, '(\r?\n){2}') with ordinality l(doc, part)
	where i.day = 5
	and l.part = 1
) i
cross join lateral regexp_split_to_table(i.doc, '\r?\n') with ordinality r(range, id)
;

create materialized view day5_number as
select i.input, cast(n as bigint) n
from (
	select *
	from input i
	cross join lateral regexp_split_to_table(i.value, '(\r?\n){2}') with ordinality l(doc, part)
	where i.day = 5
	and l.part = 2
) i
cross join lateral regexp_split_to_table(i.doc, '\r?\n') n
;


-- Calculate the solution for part 1
--------------------------------------
select n.input, count(*)
from day5_number n
where exists (select * from day5_range r where r.input = n.input and n.n between r.start and r.end)
group by n.input
;


-- Calculate the solution for part 2
--------------------------------------

with recursive r as (
	select distinct r.input, r.start, r.end, 0 level
	from day5_range r
	union all
	(
		with r_ as (select * from r),
		result as (
			select distinct a.input, least(a.start, b.start) start, greatest(a.end, b.end) "end", a.level + 1
			from r_ a
			left join r_ b on (
				a.input = b.input
				and (a.start, a.end) <> (b.start, b.end)
				and (
					a.start between b.start and b.end or
					a.end between b.start and b.end or
					b.start between a.start and a.end or
					b.end between a.start and a.end
				)
			)
		)
		select *
		from result rr
		-- only continue if there is a delta between r_ and result
		where exists (
			select a.input, a.start, a.end
			from r_ a
			except
			select a.input, a.start, a.end
			from result a
		)
		or exists (
			select a.input, a.start, a.end
			from result a
			except
			select a.input, a.start, a.end
			from r_ a
		)
	)
)
select input, sum("end" - start + 1)
from r
where level = (select max(level) from r)
group by input
;



