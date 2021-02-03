select fpe."SHIPMENT_ID",
       mv.sku,
       mv.quantity sku_units_in_shipment,
       shipment_skus.total_skus_in_shipment,
       "PRIORITY_CODE_1",
       "WORK_TYPE",
       "REQUESTED_SHIP_METHOD",
       "EXPECTED_DELIVERY_DATE",
       "EXPECTED_DAYS_IN_TRANSIT",
       "AUTHORIZED_date",
       "AGE_IN_DAYS",
       "AUTHORIZED_time",
       all_reno_skus,
       all_indy_skus,
       "IS_GIFT?",
       description,
       "Master Inventory",
       "Dock Transit To Reno",
       "Reno Staging",
       "Reno",
       "Indy Transit",
       "Indy Stage",
       "Indy",
       "Indy Putaway",
       "BK Docks",
       "Brooklyn"
from   (select shipment_id,
               sku,
               quantity
        from   mv_exporting_order_shipment_skus) mv
       join (select shipment_id,
                    count(distinct sku) total_skus_in_shipment
             from   mv_exporting_order_shipment_skus
             group  by shipment_id) shipment_skus
         on mv.shipment_id = shipment_skus.shipment_id
       join (SELECT SUB."SHIPMENT_ID",
                    SUB."PRIORITY_CODE_1",
                    SUB."WORK_TYPE",
                    SUB."REQUESTED_SHIP_METHOD",
                    SUB."EXPECTED_DELIVERY_DATE",
                    SUB."EXPECTED_DAYS_IN_TRANSIT",
                    SUB."AUTHORIZED_date",
                    SUB."AGE_IN_DAYS",
                    SUB."AUTHORIZED_time",
                    CASE
                      WHEN reno_test.nonrenosku = reno_test.renosku THEN 'Yes'
                      ELSE 'No'
                    END all_reno_skus,
                    CASE
                      WHEN indy_test.nonindysku = indy_test.indysku THEN 'Yes'
                      ELSE 'No'
                    END all_indy_skus,
                    CASE
                      WHEN sub.is_gb = 1 THEN 'Yes'
                      ELSE 'No'
                    END "IS_GIFT?"
             FROM   (SELECT DISTINCT EF.shipment_id
                                     AS
                                             "SHIPMENT_ID"
                                             ,
                                     EF.priority_code
                                             AS
                                              "PRIORITY_CODE_1",
                                     EWT.work_type
                                     AS
                                     "WORK_TYPE",
                                     PSL.requested_ship_method
                                     AS
                                              "REQUESTED_SHIP_METHOD",
                                     To_char(PSL.expected_delivery_date,
                                     'MM/DD/YY') AS
                                              "EXPECTED_DELIVERY_DATE",
                                     To_char(PSL.expected_send_date, 'MM/DD/YY')
                                     AS
                                              "EXPECTED_SEND_DATE",
                                     PSL.expected_days_in_transit
                                     AS
                                              "EXPECTED_DAYS_IN_TRANSIT",
                                     To_char(S.authorized, 'MM/DD/YY')
                                     AS
                                              "AUTHORIZED_date",
                                     Round(( Extract(epoch FROM ( localtimestamp
                                                                  -
s.authorized )) /
86400 ) ::
NUMERIC, 2)
"AGE_IN_DAYS",
To_char(S.authorized, 'HH:MM:SS AM')            AS
"AUTHORIZED_time",
ef.is_gb
FROM   exporting_work_type EWT,
exporting_flags EF
inner join exporting_shipments ES
ON EF.shipment_id = ES.shipment_id
AND ES.requeue = 1
AND ES.requeue_processed = 0
inner join shipment S
ON EF.shipment_id = S.shipment_id
inner join orders O
ON S.order_id = O.order_id
inner join proship_shipment_lookup PSL
ON EF.shipment_id = PSL.shipment_id
WHERE  S.is_backorder = 0
AND EF.work_type_id = EWT.id
AND S.is_drop_ship = 0
AND S.is_cancelled = 0
AND S.is_hold = 0
AND S.is_temp_hold = 0
AND O.is_cancelled = 0
AND O.is_hold = 0
AND O.is_temp_hold = 0
AND S.imported_date IS NULL
/*added to exclude shipments that have been exported OPS-5302*/
AND S.shipped_date IS NULL
/*added to exclude shipments that have been shipped OPS-5302*/
AND O.date_created > ( localtimestamp - ( 365
    || ' days' ) ::
interval )
AND EWT.id = 38
UNION ALL
SELECT DISTINCT EF.shipment_id                                  AS
"SHIPMENT_ID",
EF.priority_code                                AS
"PRIORITY_CODE_1",
EWT.work_type                                   AS
"WORK_TYPE",
PSL.requested_ship_method                       AS
"REQUESTED_SHIP_METHOD",
To_char(PSL.expected_delivery_date, 'MM/DD/YY') AS
"EXPECTED_DELIVERY_DATE",
To_char(PSL.expected_send_date, 'MM/DD/YY')     AS
"EXPECTED_SEND_DATE",
PSL.expected_days_in_transit                    AS
"EXPECTED_DAYS_IN_TRANSIT",
To_char(S.authorized, 'MM/DD/YY')               AS
"AUTHORIZED"
,
Round(( Extract(epoch FROM ( localtimestamp
- s.authorized )) /
86400 ) ::
NUMERIC, 2)
"AGE_IN_DAYS",
To_char(S.authorized, 'HH:MM:SS AM')            AS
"AUTHORIZED"
,
ef.is_gb
FROM   exporting_work_type EWT,
exporting_flags EF
inner join shipment S
ON EF.shipment_id = S.shipment_id
inner join orders O
ON S.order_id = O.order_id
inner join proship_shipment_lookup PSL
ON EF.shipment_id = PSL.shipment_id
WHERE  S.is_backorder = 0
AND EF.work_type_id = EWT.id
AND S.is_drop_ship = 0
AND S.is_cancelled = 0
AND S.is_hold = 0
AND S.is_temp_hold = 0
AND O.is_cancelled = 0
AND O.is_hold = 0
AND O.is_temp_hold = 0
AND S.imported_date IS NULL
AND S.shipped_date IS NULL
AND O.date_created > ( localtimestamp - ( 365
    || ' days' ) ::
interval )
AND EWT.id = 38) sub
left join (SELECT s.shipment_id,
Count(DISTINCT osk.sku) nonrenosku,
SUM(rs.reno_enable)     renosku,
CASE
WHEN Count(DISTINCT osk.sku) = 1 THEN
'SINGLE-LINE'
ELSE 'MULTI-LINE'
END                     work_type
FROM   shipment s
join order_shipment osh
ON s.shipment_id = osh.shipment_id
join order_sku osk
ON osh.order_sku_id = osk.order_sku_id
join (SELECT sku.sku,
"enable" reno_enable
FROM   sku
left outer join reno_sku rs
ON sku.sku = rs.sku
WHERE  sku.is_bits = 0
AND sku.is_drop_ship = 0) rs
ON osk.sku = rs.sku
WHERE  s.shipped_date IS NULL
AND s.is_cancelled = 0
AND s.is_hold = 0
AND s.authorized >= current_date - 120
GROUP  BY s.shipment_id) reno_test
ON sub."SHIPMENT_ID" = reno_test.shipment_id
left join (SELECT s.shipment_id,
Count(DISTINCT osk.sku) nonindysku,
SUM(ind.indy_enable)    indysku,
CASE
WHEN SUM(ind.indy_enable) = 1
AND Count(DISTINCT osk.sku) = 1 THEN
'SINGLE-LINE'
ELSE 'MULTI-LINE'
END                     work_type
FROM   shipment s
join order_shipment osh
ON s.shipment_id = osh.shipment_id
join order_sku osk
ON osh.order_sku_id = osk.order_sku_id
join (SELECT sku.sku,
indy_enable
FROM   sku
left outer join
(SELECT sku,
CASE
WHEN
quantity > 1 THEN
1
ELSE 0
END AS indy_enable
FROM   inventory_summary
WHERE  location_type_id = 8) rs
ON sku.sku = rs.sku
WHERE  sku.is_bits = 0
AND sku.is_drop_ship = 0) ind
ON osk.sku = ind.sku
WHERE  s.shipped_date IS NULL
AND s.is_cancelled = 0
AND s.is_hold = 0
AND s.authorized >= current_date - 120
GROUP  BY s.shipment_id) indy_test
ON sub."SHIPMENT_ID" = indy_test.shipment_id) fpe
on mv.shipment_id = fpe."SHIPMENT_ID"
left join (SELECT
sk.sku,
CASE
WHEN Upper(sk.color) LIKE '%NBSP%'
OR sk.color LIKE '%#160%' THEN sk.sze
ELSE sk.sze
|| ' '
|| sk.color
END                                              AS
description
,
sk.inventory
"Master Inventory",
COALESCE(threepl_dock."Dock Transit To Reno", 0)
"Dock Transit To Reno"
,
COALESCE(threepl_dock."Reno Staging", 0)
"Reno Staging",
COALESCE(reno_inv.inventory, 0)                  "Reno",
COALESCE(threepl_dock."Indy Transit", 0)
"Indy Transit",
COALESCE(threepl_dock."Indy Stage", 0)           "Indy Stage",
COALESCE(indy_inv.inventory, 0)                  "Indy",
COALESCE(indypa_inv.inventory, 0)
"Indy Putaway",
COALESCE(threepl_dock."BK Dock", 0)              "BK Docks",
COALESCE(bk_inv.inventory, 0)                    "Brooklyn"
FROM   sku sk
left join (SELECT Sum(invs.quantity) inventory,
invs.sku
FROM   inventory_summary invs
WHERE  invs.location_type_id = 6
AND invs.loc_max_units <> 0
GROUP  BY invs.sku) reno_inv
ON reno_inv.sku = sk.sku
left join (SELECT Sum(invs.quantity) inventory,
invs.sku
FROM   inventory_summary invs
WHERE  invs.location_type_id = 8
AND invs.loc_max_units <> 0
GROUP  BY invs.sku) indy_inv
ON indy_inv.sku = sk.sku
left join (SELECT Sum(invs.quantity) inventory,
invs.sku
FROM   inventory_summary invs
WHERE  invs.location_type_id = 9
AND invs.loc_max_units <> 0
GROUP  BY invs.sku) indypa_inv
ON indypa_inv.sku = sk.sku
left join (SELECT Sum(invs.quantity) inventory,
invs.sku
FROM   inventory_summary invs
WHERE  invs.location_type_id IN ( 1, 2, 3, 4, 7 )
AND invs.loc_max_units <> 0
GROUP  BY invs.sku) bk_inv
ON bk_inv.sku = sk.sku
left join (SELECT inv.sku,
Sum(inv.quantity)        pltdtr,
Count(inv.container_key) AS number_pallet
FROM   inventory_summary inv,
inventory_work_queue iwq
WHERE  1 = 1
AND iwq.container_key = inv.container_key
AND iwq.completed_date IS NULL
AND iwq.sku = inv.sku
AND iwq.cancelled_date IS NULL
AND iwq.started_date IS NOT NULL
AND inv.loc_max_units > 0
AND inv.loc_max_units IS NOT NULL
AND inv.location_type_id = 11
AND inv.sku < 800000000000
AND ( iwq.end_location IS NOT NULL
OR iwq.work_type_id IN
( 10, 4, 9, 1 ) )
AND inv.id IN
(SELECT inventory_summary_id
FROM   wh_storage_unit su
WHERE  su.parent_storage_unit_id IN (
384698 ))
GROUP  BY inv.sku
ORDER  BY inv.sku) bk_dock
ON bk_dock.sku = sk.sku
full outer join (SELECT a1.sku,
sum(a1."Dock Transit To Reno")
"Dock Transit To Reno",
sum(a1."Reno Staging")
"Reno Staging",
sum(a1."Indy Transit")
"Indy Transit",
sum(a1."Indy Stage")
"Indy Stage",
sum(a1."BK Dock")
"BK Dock"
FROM   (SELECT dock.sku,
COALESCE(
sum(CASE
WHEN dock.dock =
'Dock Transit To Reno' THEN
coalesce(dock.units, 0)
END), 0) "Dock Transit To Reno",
COALESCE(sum(CASE
      WHEN
dock.dock =
'Reno Staging'
    THEN
coalesce(dock.units, 0)
END), 0) "Reno Staging",
COALESCE(sum(CASE
WHEN dock.dock =
'Indy Transit'
THEN
coalesce(dock.units, 0)
END), 0) "Indy Transit",
COALESCE(sum(CASE
WHEN dock.dock =
'Indy Stage'
THEN
coalesce(dock.units, 0)
END), 0) "Indy Stage",
COALESCE(
sum(CASE
WHEN dock.dock in (
'Dock Returns',
'Dock 5',
'Dock 2',
'Dock 7',
'Dock 5 Overflow',
'Dock 7 Overflow',
'Dock 2 Overflow',
'Dock 2 Prod'
,
'Dock 2 Pre-pack',
'Dock Bin Audit',
'Dock Rebalance From Reno',
'Rebalance Indy',
'Backorder', 'IN TRANSIT' )
THEN
coalesce(dock.units, 0)
END), 0) "BK Dock"
FROM   (SELECT COALESCE(wsu2.name, 'IN TRANSIT'
) AS
dock,
invs.sku
AS sku
,
invs.quantity                     AS units
FROM   inventory_summary invs
INNER JOIN wh_storage_unit wsu
ON
invs.id = wsu.inventory_summary_id
LEFT JOIN wh_storage_unit wsu2
ON wsu.parent_storage_unit_id =
wsu2.id
LEFT JOIN wh_storage_unit wsu3
ON wsu2.parent_storage_unit_id =
wsu3.id
INNER JOIN inventory_work_queue iwq
ON iwq.container_key =
invs.container_key
AND invs.sku = iwq.sku
INNER JOIN inventory_transaction it
ON
iwq.id = it.inventory_work_queue_id
INNER JOIN (SELECT Max(it.id) AS KEY,
it.inventory_work_queue_id
FROM   inventory_work_queue iwq,
inventory_transaction it
inner join administrators ad
ON ( it.created_admin_id =
ad.id )
WHERE  it.inventory_work_queue_id = iwq.id
and iwq.sku < 900000000000
and iwq.quantity > 0
AND Coalesce(iwq.cancelled_date ::
text, '')
= ''
AND Coalesce(iwq.completed_date ::
text, '')
= ''
AND iwq.completed_date IS NULL
AND iwq.cancelled_date IS NULL
AND iwq.started_date IS NOT NULL
GROUP  BY it.inventory_work_queue_id) invt
ON it.id = invt.KEY
LEFT OUTER JOIN (SELECT sk.sku            AS KEY,
inv.container_key loc,
inv.quantity
FROM   inventory_summary inv,
wh_storage_unit wsu,
sku sk
WHERE
inv.id = wsu.inventory_summary_id
AND sk.sku = inv.sku
AND inv.location_type_id = 1
AND ( wsu.loc1 IS NOT NULL
AND wsu.loc1 :: text <> '' )
GROUP  BY sk.sku,
inv.container_key,
inv.quantity
ORDER  BY sk.sku) f
ON invs.sku = f.KEY
LEFT OUTER JOIN (SELECT sk.sku            AS KEY,
inv.container_key loc,
inv.quantity
FROM   inventory_summary inv,
wh_storage_unit wsu,
sku sk
WHERE
inv.id = wsu.inventory_summary_id
AND sk.sku = inv.sku
AND inv.location_type_id = 7
AND ( wsu.loc1 IS NOT NULL
AND wsu.loc1 :: text <> '' )
GROUP  BY sk.sku,
inv.container_key,
inv.quantity
ORDER  BY sk.sku) mach
ON invs.sku = mach.KEY
LEFT JOIN sku_sls_max_3_day_by_fwd doh
ON invs.sku = doh.sku
RIGHT JOIN sku sk
ON sk.sku = invs.sku
INNER JOIN administrators ad1
ON it.completed_admin_id = ad1.id
INNER JOIN administrators ad2
ON it.created_admin_id = ad2.id
WHERE  invs.location_type_id = 11
AND iwq.completed_date IS NULL
AND iwq.cancelled_date IS NULL
AND iwq.started_date IS NOT NULL
AND invs.sku < 800000000000
AND invs.quantity > 0
AND wsu.parent_storage_unit_id NOT IN ( 41179 )
AND ( iwq.end_location IS NOT NULL
OR iwq.work_type_id IN ( 10, 4, 9, 1 ) )) DOCK
GROUP  BY dock.sku,
dock.dock) a1
GROUP  BY a1.sku) threepl_dock
ON threepl_dock.sku = sk.sku
WHERE  reno_inv.inventory IS NOT NULL
OR indy_inv.inventory IS NOT NULL
OR indypa_inv.inventory IS NOT NULL
OR bk_inv.inventory IS NOT NULL
OR threepl_dock."Dock Transit To Reno" IS NOT NULL
OR threepl_dock."Reno Staging" IS NOT NULL
OR threepl_dock."Indy Transit" IS NOT NULL
OR threepl_dock."Indy Stage" IS NOT NULL
OR threepl_dock."Reno Staging" IS NOT NULL
OR threepl_dock."BK Dock" IS NOT NULL) all_warehouse_inventory
on all_warehouse_inventory.sku = mv.sku
order  by fpe."EXPECTED_DELIVERY_DATE" 