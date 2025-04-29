def process_product_creation():
    sql = """
    INSERT INTO product_center.products(product_id, name, description, category, price, stock, status)
    SELECT 
        product_id, 
        product_name as name, 
        product_description as description, 
        category, 
        price, 
        stock_quantity as stock, 
        product_status as status
    FROM raw_data.product_logs
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_product_images():
    sql = """
    INSERT INTO product_center.product_images(image_id, product_id, image_url, is_primary, sort_order)
    SELECT 
        image_id, 
        product_id, 
        image_url, 
        is_main as is_primary, 
        display_order as sort_order
    FROM raw_data.product_images
    WHERE upload_time >= '{self.date}' AND upload_time < '{self.date_next}'
    """
    return sql

def process_product_attributes():
    sql = """
    INSERT INTO product_center.product_attributes(product_id, attribute_name, attribute_value)
    SELECT 
        product_id, 
        attribute_key as attribute_name, 
        attribute_value
    FROM raw_data.product_attributes
    WHERE update_time >= '{self.date}' AND update_time < '{self.date_next}'
    """
    return sql

def process_product_inventory():
    sql = """
    INSERT INTO inventory.product_inventory(product_id, warehouse_id, quantity, last_updated)
    SELECT 
        product_id, 
        warehouse_id, 
        stock_quantity as quantity, 
        update_time as last_updated
    FROM raw_data.inventory_logs
    WHERE update_time >= '{self.date}' AND update_time < '{self.date_next}'
    """
    return sql

def process_product_price_history():
    sql = """
    INSERT INTO product_center.price_history(product_id, price, effective_from, effective_to, change_reason)
    SELECT 
        product_id, 
        new_price as price, 
        change_time as effective_from, 
        LEAD(change_time) OVER (PARTITION BY product_id ORDER BY change_time) as effective_to,
        change_reason
    FROM raw_data.price_change_logs
    WHERE change_time >= '{self.date}' AND change_time < '{self.date_next}'
    """
    return sql

def process_product_reviews():
    sql = """
    INSERT INTO product_center.product_reviews(review_id, product_id, user_id, rating, review_text, review_time)
    SELECT 
        review_id, 
        product_id, 
        user_id, 
        rating, 
        comment as review_text, 
        create_time as review_time
    FROM raw_data.product_reviews
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_product_sales():
    sql = """
    INSERT INTO analytics.product_sales(date, product_id, units_sold, revenue, avg_rating)
    SELECT 
        DATE(o.order_time) as date,
        i.product_id,
        SUM(i.quantity) as units_sold,
        SUM(i.subtotal) as revenue,
        (
            SELECT AVG(rating)
            FROM product_center.product_reviews
            WHERE product_id = i.product_id
        ) as avg_rating
    FROM order_center.orders o
    JOIN order_center.order_items i ON o.order_id = i.order_id
    WHERE o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
    GROUP BY DATE(o.order_time), i.product_id
    """
    return sql

def process_category_performance():
    sql = """
    INSERT INTO analytics.category_performance(date, category, total_sales, product_count, avg_price)
    SELECT 
        DATE(o.order_time) as date,
        p.category,
        SUM(i.subtotal) as total_sales,
        COUNT(DISTINCT i.product_id) as product_count,
        AVG(i.unit_price) as avg_price
    FROM order_center.orders o
    JOIN order_center.order_items i ON o.order_id = i.order_id
    JOIN product_center.products p ON i.product_id = p.product_id
    WHERE o.order_time >= '{self.date}' AND o.order_time < '{self.date_next}'
    GROUP BY DATE(o.order_time), p.category
    """
    return sql

def update_product_rating():
    sql = """
    UPDATE product_center.products p
    SET average_rating = (
        SELECT AVG(rating)
        FROM product_center.product_reviews
        WHERE product_id = p.product_id
    )
    WHERE EXISTS (
        SELECT 1
        FROM product_center.product_reviews
        WHERE product_id = p.product_id
        AND review_time >= '{self.date}' AND review_time < '{self.date_next}'
    )
    """
    return sql