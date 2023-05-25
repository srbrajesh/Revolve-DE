select
	"AIRPORT",
	sum(flight) as flight_sum
from
	airports a
inner join flights f 
on
	a."IATA_CODE" = f.origin
where
	day in (6)
group by
	"AIRPORT"
order by
	flight_sum desc
limit 1