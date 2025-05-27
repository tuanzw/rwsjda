select distinct odh.order_id, odh.status,
case(odh.status)
when 'Released' then 'New order released, waiting for picking'
when 'Picked' then 'Order finshed picking, waiting for repacking'
when 'Repacked' then 'Order finished repacking, waiting for marshal'
when 'Complete' then 'Order finished marshal, waiting for issuing Invoice and Packing list'
when 'Allocated' then 'Waiting for picking'
else odh.status
end as remarks
from order_header odh
inner join order_line odl
on odh.order_id = odl.order_id
inner join sku on 
odl.client_id = sku.client_id and odl.sku_id=sku.sku_id
where odh.client_id='FISCHER'
and odh.from_site_id='VNHCM04'
and odh.status <> 'Shipped'
and sku.hazmat='Y'
order by odh.status
