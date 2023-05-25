with manufacturer_planes as(select * from public.planes p
inner join
public.flights f 
on p.tailnum = f.tailnum)
select manufacturer, sum(distance) as distance_grouped
from manufacturer_planes
group by manufacturer order by distance_grouped desc limit 1