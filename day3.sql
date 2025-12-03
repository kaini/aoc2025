-- Parse the input
--------------------------------------
create materialized view day3_input as
select input, line_no, chr_no, cast(c as int) n
from input i
cross join lateral string_to_table(i.value, E'\n') with ordinality x(line, line_no)
cross join lateral regexp_split_to_table(trim(line, E'\n\r '), '') with ordinality y(c, chr_no)
where i.day = 3
;

create index day3_a on day3_input (input, line_no, chr_no, n);


-- Calculate the solution for part 1
--------------------------------------
select lines.input, sum(lines.max)
from (
	select a.input, a.line_no, max(a.n * 10 + b.n) max
	from day3_input a
	join day3_input b on b.input = a.input and b.line_no = a.line_no and b.chr_no > a.chr_no
	group by a.input, a.line_no
) lines
group by lines.input
;


-- Calculate the solution for part 2
--------------------------------------

-- Find the highest digit of each number incrementally from left to right (with enough space to the right for the remaining numbers)
with recursive result as (
	select *, 1 depth
	from day3_input i
	where (i.n, -i.chr_no) >= all (select ii.n, -ii.chr_no from day3_input ii where ii.input = i.input and ii.line_no = i.line_no and ii.chr_no <= (select max(l.chr_no) - 11 from day3_input l where l.input = i.input and l.line_no = i.line_no))
	and i.chr_no <= (select max(l.chr_no) - 11 from day3_input l where l.input = i.input and l.line_no = i.line_no)
	union all
	select i.*, r.depth + 1
	from result r
	join day3_input i on i.input = r.input and i.line_no = r.line_no and i.chr_no > r.chr_no
	where r.depth < 12
	and (i.n, -i.chr_no) >= all (select ii.n, -ii.chr_no from day3_input ii where ii.input = i.input and ii.line_no = i.line_no and ii.chr_no > r.chr_no and ii.chr_no <= (select max(l.chr_no) - (11 - r.depth) from day3_input l where l.input = i.input and l.line_no = i.line_no))
	and i.chr_no <= (select max(l.chr_no) - (11 - r.depth) from day3_input l where l.input = i.input and l.line_no = i.line_no)
)
select input, sum(n)
from (
	select input, line_no, cast(string_agg(cast(n as text), '' order by chr_no) as bigint) n
	from result
	group by input, line_no
)
group by input
;

