with manufacturer_planes as(select * from public.planes p
inner join
public.flights f 
on p.tailnum = f.tailnum)
select dest, sum(cast(dep_delay as int)) as dep_delay_grouped from manufacturer_planes
where dep_delay != 'NA'
group by dest
order by dep_delay_grouped desc limit 1