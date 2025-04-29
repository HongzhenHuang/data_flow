def process_order_creation():
    sql = """
    INSERT INTO order_center.orders(order_id, user_id, order_time, total_amount, status, payment_method)
    SELECT 
        order_id, 
        user_id, 
        create_time as order_time, 
        total_price as total_amount, 
        order_status as status,
        payment_type as payment_method
    FROM raw_data.order_logs
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_order_items():
    sql = """
    INSERT INTO order_center.order_items(order_id, product_id, quantity, unit_price, subtotal)
    SELECT 
        order_id, 
        product_id, 
        quantity, 
        price as unit_price, 
        quantity * price as subtotal
    FROM raw_data.order_item_details
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_order_payment():
    sql = """
    INSERT INTO payment_center.payments(payment_id, order_id, user_id, amount, payment_time, status)
    SELECT 
        payment_id, 
        order_id, 
        user_id, 
        payment_amount as amount, 
        payment_time, 
        payment_status as status
    FROM raw_data.payment_logs
    WHERE payment_time >= '{self.date}' AND payment_time < '{self.date_next}'
    """
    return sql

def process_order_shipping():
    sql = """
    INSERT INTO logistics.shipments(shipment_id, order_id, shipping_address, shipping_method, tracking_number, status)
    SELECT 
        s.shipment_id, 
        s.order_id, 
        a.full_address as shipping_address, 
        s.shipping_method, 
        s.tracking_number, 
        s.status
    FROM raw_data.shipping_records s
    JOIN user_center.user_addresses a ON s.address_id = a.address_id
    WHERE s.create_time >= '{self.date}' AND s.create_time < '{self.date_next}'
    """
    return sql

def process_order_refund():
    sql = """
    INSERT INTO payment_center.refunds(refund_id, order_id, payment_id, amount, refund_time, reason, status)
    SELECT 
        refund_id, 
        order_id, 
        payment_id, 
        refund_amount as amount, 
        refund_time, 
        refund_reason as reason, 
        refund_status as status
    FROM raw_data.refund_logs
    WHERE refund_time >= '{self.date}' AND refund_time < '{self.date_next}'
    """
    return sql

def process_daily_order_summary():
    sql = """
    INSERT INTO analytics.daily_order_summary(date, total_orders, total_amount, avg_order_value, payment_method_distribution)
    SELECT 
        DATE(o.order_time) as date, 
        COUNT(DISTINCT o.order_id) as total_orders, 
        SUM(o.total_amount) as total_amount, 
        AVG(o.total_amount) as avg_order_value,
        JSON_OBJECTAGG(o.payment_method, COUNT(*)) as payment_method_distribution
    FROM order_center.orders o
    WHERE o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
    GROUP BY DATE(o.order_time)
    """
    return sql

def process_user_order_stats():
    sql = """
    INSERT INTO analytics.user_order_stats(user_id, total_orders, total_spent, first_order_date, last_order_date, favorite_category)
    SELECT 
        o.user_id, 
        COUNT(DISTINCT o.order_id) as total_orders, 
        SUM(o.total_amount) as total_spent,
        MIN(DATE(o.order_time)) as first_order_date,
        MAX(DATE(o.order_time)) as last_order_date,
        (
            SELECT p.category
            FROM order_center.order_items oi
            JOIN product_center.products p ON oi.product_id = p.product_id
            WHERE oi.order_id IN (SELECT order_id FROM order_center.orders WHERE user_id = o.user_id)
            GROUP BY p.category
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ) as favorite_category
    FROM order_center.orders o
    WHERE o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
    GROUP BY o.user_id
    """
    return sql

def process_order_conversion():
    sql = """
    INSERT INTO analytics.order_conversion(date, cart_additions, checkout_starts, orders_completed, conversion_rate)
    SELECT 
        DATE(e.event_time) as date,
        SUM(CASE WHEN e.event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart_additions,
        SUM(CASE WHEN e.event_type = 'begin_checkout' THEN 1 ELSE 0 END) as checkout_starts,
        COUNT(DISTINCT o.order_id) as orders_completed,
        COUNT(DISTINCT o.order_id) / SUM(CASE WHEN e.event_type = 'begin_checkout' THEN 1 ELSE 0 END) as conversion_rate
    FROM raw_data.user_events e
    LEFT JOIN order_center.orders o ON e.user_id = o.user_id AND DATE(e.event_time) = DATE(o.order_time)
    WHERE e.event_time >= '{self.date}' AND e.event_time < '{self.date_next}'
    GROUP BY DATE(e.event_time)
    """
    return sql

def update_order_status():
    sql = """
    UPDATE order_center.orders
    SET status = 'delivered'
    WHERE order_id IN (
        SELECT order_id 
        FROM logistics.shipments
        WHERE status = 'delivered' 
        AND delivery_time >= '{self.date}' AND delivery_time < '{self.date_next}'
    )
    """
    return sql