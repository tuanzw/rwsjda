select distinct odh.order_id, odl.sku_id, sku.description, odl.qty_ordered, 
odl.qty_ordered*sku.each_weight as gross_weight, skc.config_id,
odl.qty_ordered / skc.ratio_1_to_2 as pallet_estimate,odh.status,
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
inner join sku_sku_config ssc on sku.sku_id = ssc.sku_id and ssc.client_id = sku.client_id
inner join sku_config skc on ssc.client_id=skc.client_id and ssc.config_id=skc.config_id
where odh.client_id='FISCHER'
and odh.from_site_id='VNHCM04'
and odh.status not in ('Shipped', 'Cancelled')
and sku.hazmat='Y'
and ssc.main_config_id='Y'
order by odh.status