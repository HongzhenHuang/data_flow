def process_warehouse_creation():
    sql = """
    INSERT INTO inventory.warehouses(warehouse_id, name, location, capacity, status)
    SELECT 
        warehouse_id, 
        warehouse_name as name, 
        warehouse_location as location, 
        storage_capacity as capacity, 
        operational_status as status
    FROM raw_data.warehouse_records
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_inventory_receipt():
    sql = """
    INSERT INTO inventory.inventory_receipts(receipt_id, warehouse_id, supplier_id, receipt_date, total_items)
    SELECT 
        receipt_id, 
        warehouse_id, 
        supplier_id, 
        receive_date as receipt_date, 
        item_count as total_items
    FROM raw_data.inventory_receipts
    WHERE receive_date >= '{self.date}' AND receive_date < '{self.date_next}'
    """
    return sql

def process_receipt_items():
    sql = """
    INSERT INTO inventory.receipt_items(receipt_id, product_id, quantity, unit_cost)
    SELECT 
        receipt_id, 
        product_id, 
        received_quantity as quantity, 
        cost_per_unit as unit_cost
    FROM raw_data.receipt_item_details
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_inventory_adjustments():
    sql = """
    INSERT INTO inventory.inventory_adjustments(adjustment_id, warehouse_id, product_id, quantity_change, reason, adjustment_time)
    SELECT 
        adjustment_id, 
        warehouse_id, 
        product_id, 
        quantity_delta as quantity_change, 
        adjustment_reason as reason, 
        adjustment_time
    FROM raw_data.inventory_adjustments
    WHERE adjustment_time >= '{self.date}' AND adjustment_time < '{self.date_next}'
    """
    return sql

def process_inventory_transfers():
    sql = """
    INSERT INTO inventory.inventory_transfers(transfer_id, source_warehouse_id, destination_warehouse_id, transfer_date, status)
    SELECT 
        transfer_id, 
        from_warehouse_id as source_warehouse_id, 
        to_warehouse_id as destination_warehouse_id, 
        transfer_date, 
        transfer_status as status
    FROM raw_data.inventory_transfers
    WHERE transfer_date >= '{self.date}' AND transfer_date < '{self.date_next}'
    """
    return sql

def process_transfer_items():
    sql = """
    INSERT INTO inventory.transfer_items(transfer_id, product_id, quantity)
    SELECT 
        transfer_id, 
        product_id, 
        transfer_quantity as quantity
    FROM raw_data.transfer_item_details
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_inventory_levels():
    sql = """
    INSERT INTO analytics.inventory_levels(date, warehouse_id, product_id, beginning_quantity, receipts, sales, adjustments, transfers_in, transfers_out, ending_quantity)
    SELECT 
        '{self.date}' as date,
        w.warehouse_id,
        p.product_id,
        COALESCE(prev.ending_quantity, 0) as beginning_quantity,
        COALESCE(SUM(ri.quantity), 0) as receipts,
        COALESCE(SUM(oi.quantity), 0) as sales,
        COALESCE(SUM(CASE WHEN ia.quantity_change > 0 THEN ia.quantity_change ELSE 0 END), 0) -
        COALESCE(SUM(CASE WHEN ia.quantity_change < 0 THEN ABS(ia.quantity_change) ELSE 0 END), 0) as adjustments,
        COALESCE(SUM(CASE WHEN ti.transfer_id IN (SELECT transfer_id FROM inventory.inventory_transfers WHERE destination_warehouse_id = w.warehouse_id) THEN ti.quantity ELSE 0 END), 0) as transfers_in,
        COALESCE(SUM(CASE WHEN ti.transfer_id IN (SELECT transfer_id FROM inventory.inventory_transfers WHERE source_warehouse_id = w.warehouse_id) THEN ti.quantity ELSE 0 END), 0) as transfers_out,
        COALESCE(prev.ending_quantity, 0) + 
        COALESCE(SUM(ri.quantity), 0) - 
        COALESCE(SUM(oi.quantity), 0) +
        COALESCE(SUM(CASE WHEN ia.quantity_change > 0 THEN ia.quantity_change ELSE 0 END), 0) -
        COALESCE(SUM(CASE WHEN ia.quantity_change < 0 THEN ABS(ia.quantity_change) ELSE 0 END), 0) +
        COALESCE(SUM(CASE WHEN ti.transfer_id IN (SELECT transfer_id FROM inventory.inventory_transfers WHERE destination_warehouse_id = w.warehouse_id) THEN ti.quantity ELSE 0 END), 0) -
        COALESCE(SUM(CASE WHEN ti.transfer_id IN (SELECT transfer_id FROM inventory.inventory_transfers WHERE source_warehouse_id = w.warehouse_id) THEN ti.quantity ELSE 0 END), 0) as ending_quantity
    FROM inventory.warehouses w
    CROSS JOIN product_center.products p
    LEFT JOIN analytics.inventory_levels prev ON prev.warehouse_id = w.warehouse_id AND prev.product_id = p.product_id AND prev.date = DATE_SUB('{self.date}', INTERVAL 1 DAY)
    LEFT JOIN inventory.receipt_items ri ON p.product_id = ri.product_id
    LEFT JOIN inventory.inventory_receipts ir ON ri.receipt_id = ir.receipt_id AND ir.warehouse_id = w.warehouse_id AND ir.receipt_date >= '{self.date}' AND ir.receipt_date < '{self.date_next}'
    LEFT JOIN order_center.order_items oi ON p.product_id = oi.product_id
    LEFT JOIN order_center.orders o ON oi.order_id = o.order_id AND o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
    LEFT JOIN inventory.inventory_adjustments ia ON p.product_id = ia.product_id AND ia.warehouse_id = w.warehouse_id AND ia.adjustment_time >= '{self.date}' AND ia.adjustment_time < '{self.date_next}'
    LEFT JOIN inventory.transfer_items ti ON p.product_id = ti.product_id
    LEFT JOIN inventory.inventory_transfers it ON ti.transfer_id = it.transfer_id AND (it.source_warehouse_id = w.warehouse_id OR it.destination_warehouse_id = w.warehouse_id) AND it.transfer_date >= '{self.date}' AND it.transfer_date < '{self.date_next}'
    GROUP BY w.warehouse_id, p.product_id
    """
    return sql

def process_low_stock_alerts():
    sql = """
    INSERT INTO inventory.low_stock_alerts(product_id, warehouse_id, current_quantity, threshold, alert_date)
    SELECT 
        i.product_id,
        i.warehouse_id,
        i.quantity as current_quantity,
        p.reorder_threshold as threshold,
        CURRENT_DATE() as alert_date
    FROM inventory.product_inventory i
    JOIN product_center.products p ON i.product_id = p.product_id
    WHERE i.quantity <= p.reorder_threshold
    AND NOT EXISTS (
        SELECT 1 FROM inventory.low_stock_alerts 
        WHERE product_id = i.product_id 
        AND warehouse_id = i.warehouse_id 
        AND alert_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    )
    """
    return sql

def update_inventory_after_order():
    sql = """
    UPDATE inventory.product_inventory i
    JOIN (
        SELECT 
            oi.product_id,
            o.warehouse_id,
            SUM(oi.quantity) as total_quantity
        FROM order_center.order_items oi
        JOIN order_center.orders o ON oi.order_id = o.order_id
        WHERE o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
        AND o.status = 'completed'
        GROUP BY oi.product_id, o.warehouse_id
    ) sold ON i.product_id = sold.product_id AND i.warehouse_id = sold.warehouse_id
    SET i.quantity = i.quantity - sold.total_quantity,
        i.last_updated = NOW()
    """
    return sql