select odl.order_id,odl.line_id,odl.sku_id,odl.tracking_level,odl.qty_ordered,odl.user_def_type_1,odl.user_def_type_2,
kl.sku_id, kl.quantity
from order_header odh
inner join order_line odl on odh.order_id=odl.order_id
left join kit_line kl on odl.client_id=kl.client_id and odl.sku_id=kl.kit_id
where odh.client_id='ZWILLING'
and odh.from_site_id='VNSORH2'
and status='Released'
and odh.order_type='MARKET'
and odh.user_def_chk_4='N'
order by odl.order_id, odl.line_id desc