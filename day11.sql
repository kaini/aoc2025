-- Parse the input
--------------------------------------
create materialized view day11 as
select i.input, substr(line, 1, strpos(line, ':') - 1) src, dest
from input i
cross join lateral regexp_split_to_table(i.value, '\r?\n') line
cross join lateral regexp_split_to_table(substr(line, strpos(line, ':') + 2), ' ') dest
where i.day = 11
;

create index day11_a on day11(input, src, dest);
create index day11_b on day11(input, dest, src);


-- Calculate the solution for part 1
--------------------------------------
with recursive path as (
	select *
	from day11
	where src = 'you'
	union all
	select l.*
	from path p
	join day11 l on l.input = p.input and l.src = p.dest
	where p.dest <> 'out'
)
select input, count(*)
from path
where dest = 'out'
group by input
;

-- Calculate the solution for part 2
--------------------------------------

-- Calculate in three steps:
-- Path from svr to fft; from fft to dac; from dac to out
-- and
-- Path from svr to dac; from dac to fft; from fft to out
-- The algorithm for each step is Bellman-Ford like.

with recursive path as (
	select
		input,
		node,
		(case node when 'svr' then 1 else 0 end)::numeric svr_tokens,
		(case node when 'svr' then 1 else 0 end)::numeric svr_count,
		(case node when 'fft' then 1 else 0 end)::numeric fft_tokens,
		(case node when 'fft' then 1 else 0 end)::numeric fft_count,
		(case node when 'dac' then 1 else 0 end)::numeric dac_tokens,
		(case node when 'dac' then 1 else 0 end)::numeric dac_count,
		0 depth
	from (select input, src node from day11 union select input, dest node from day11)
	union all
	(
		with path as (select * from path),
		next as (
			select
				p.input,
				p.node,
				(
					-- incoming edges
					(select coalesce(sum(pp.svr_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) svr_tokens,
				(
					p.svr_count
					+
					-- incoming
					(select coalesce(sum(pp.svr_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) svr_count,
				(
					-- incoming edges
					(select coalesce(sum(pp.fft_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) fft_tokens,
				(
					p.fft_count
					+
					-- incoming
					(select coalesce(sum(pp.fft_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) fft_count,
				(
					-- incoming edges
					(select coalesce(sum(pp.dac_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) dac_tokens,
				(
					p.dac_count
					+
					-- incoming
					(select coalesce(sum(pp.dac_tokens), 0) from path pp where pp.input = p.input and pp.node in (select l.src from day11 l where l.input = p.input and l.dest = p.node))
				) dac_count,
				p.depth + 1 depth
			from path p
		)
		select *
		from next n
		-- only continue if we did not find a fixed point
		where exists (select input, node, svr_tokens, fft_tokens, dac_tokens from path except select input, node, svr_tokens, fft_tokens, dac_tokens from next)
		or exists (select input, node, svr_tokens, fft_tokens, dac_tokens from next except select input, node, svr_tokens, fft_tokens, dac_tokens from path)
	)
),
result as (
	select *
	from path p
	where p.depth = (select max(depth) from path)
	order by node
)
select
	i.input,
	(
		(select svr_count from result where node = 'fft' and input = i.input) * (select fft_count from result where node = 'dac' and input = i.input) * (select dac_count from result where node = 'out' and input = i.input)
		+
		(select svr_count from result where node = 'dac' and input = i.input) * (select dac_count from result where node = 'fft' and input = i.input) * (select fft_count from result where node = 'out' and input = i.input) 
	)
from input i
where i.day = 11
;


