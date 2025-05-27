select trim(leading '0' from sku_id) article,
description as name,to_char(expiry_dstamp + 7/24,'YYYY-Mon-DD') as expiry_date,
sum(qty_on_hand) as qty,
trunc(expiry_dstamp) - trunc(sysdate) as shelf_life
from inventory
where client_id='FISCHER'
and site_id='VNHCM04'
and zone_1 in ('DSH1', 'MHD1')
and expiry_dstamp is not null
and sysdate + 361 > expiry_dstamp
group by sku_id, description, expiry_dstamp
