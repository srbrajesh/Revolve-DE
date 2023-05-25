with manufacturer_planes as(select * from public.planes p
inner join
public.flights f 
on p.tailnum = f.tailnum)
select manufacturer, sum(cast(hour as int)) as flying_time from manufacturer_planes
where hour != 'NA'
group by manufacturer 
order by flying_time desc limit 1