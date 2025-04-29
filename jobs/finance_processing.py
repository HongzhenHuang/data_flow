def process_revenue_recognition():
    sql = """
    INSERT INTO finance.revenue(order_id, user_id, amount, revenue_date, payment_method)
    SELECT 
        o.order_id, 
        o.user_id, 
        o.total_amount as amount, 
        p.payment_time as revenue_date, 
        o.payment_method
    FROM order_center.orders o
    JOIN payment_center.payments p ON o.order_id = p.order_id
    WHERE p.status = 'completed'
    AND p.payment_time >= '{self.date}' AND p.payment_time < '{self.date_next}'
    """
    return sql

def process_refund_expense():
    sql = """
    INSERT INTO finance.expenses(expense_id, expense_type, amount, expense_date, reference_id)
    SELECT 
        r.refund_id as expense_id, 
        'refund' as expense_type, 
        r.amount, 
        r.refund_time as expense_date, 
        r.order_id as reference_id
    FROM payment_center.refunds r
    WHERE r.status = 'completed'
    AND r.refund_time >= '{self.date}' AND r.refund_time < '{self.date_next}'
    """
    return sql

def process_shipping_expense():
    sql = """
    INSERT INTO finance.expenses(expense_id, expense_type, amount, expense_date, reference_id)
    SELECT 
        s.shipment_id as expense_id, 
        'shipping' as expense_type, 
        s.shipping_cost as amount, 
        s.shipping_date as expense_date, 
        s.order_id as reference_id
    FROM logistics.shipments s
    WHERE s.shipping_date >= '{self.date}' AND s.shipping_date < '{self.date_next}'
    """
    return sql

def process_inventory_expense():
    sql = """
    INSERT INTO finance.expenses(expense_id, expense_type, amount, expense_date, reference_id)
    SELECT 
        r.receipt_id as expense_id, 
        'inventory' as expense_type, 
        SUM(ri.quantity * ri.unit_cost) as amount, 
        r.receipt_date as expense_date, 
        r.supplier_id as reference_id
    FROM inventory.inventory_receipts r
    JOIN inventory.receipt_items ri ON r.receipt_id = ri.receipt_id
    WHERE r.receipt_date >= '{self.date}' AND r.receipt_date < '{self.date_next}'
    GROUP BY r.receipt_id, r.receipt_date, r.supplier_id
    """
    return sql

def process_marketing_expense():
    sql = """
    INSERT INTO finance.expenses(expense_id, expense_type, amount, expense_date, reference_id)
    SELECT 
        c.campaign_id as expense_id, 
        'marketing' as expense_type, 
        c.budget_spent as amount, 
        c.end_date as expense_date, 
        c.channel_id as reference_id
    FROM marketing.campaigns c
    WHERE c.end_date >= '{self.date}' AND c.end_date < '{self.date_next}'
    """
    return sql

def process_monthly_profit():
    sql = """
    INSERT INTO finance.profit_summary(month, year, revenue, expenses, profit, profit_margin)
    SELECT 
        MONTH(r.revenue_date) as month,
        YEAR(r.revenue_date) as year,
        SUM(r.amount) as revenue,
        COALESCE(e.total_expenses, 0) as expenses,
        SUM(r.amount) - COALESCE(e.total_expenses, 0) as profit,
        (SUM(r.amount) - COALESCE(e.total_expenses, 0)) / SUM(r.amount) * 100 as profit_margin
    FROM finance.revenue r
    LEFT JOIN (
        SELECT 
            MONTH(expense_date) as month,
            YEAR(expense_date) as year,
            SUM(amount) as total_expenses
        FROM finance.expenses
        WHERE expense_date >= '{self.date}' AND expense_date < '{self.date_next}'
        GROUP BY MONTH(expense_date), YEAR(expense_date)
    ) e ON MONTH(r.revenue_date) = e.month AND YEAR(r.revenue_date) = e.year
    WHERE r.revenue_date >= '{self.date}' AND r.revenue_date < '{self.date_next}'
    GROUP BY MONTH(r.revenue_date), YEAR(r.revenue_date), e.total_expenses
    """
    return sql

def process_customer_lifetime_value():
    sql = """
    INSERT INTO analytics.customer_ltv(user_id, total_revenue, first_purchase_date, last_purchase_date, purchase_count)
    SELECT 
        r.user_id,
        SUM(r.amount) as total_revenue,
        MIN(r.revenue_date) as first_purchase_date,
        MAX(r.revenue_date) as last_purchase_date,
        COUNT(DISTINCT r.order_id) as purchase_count
    FROM finance.revenue r
    GROUP BY r.user_id
    HAVING MAX(r.revenue_date) >= '{self.date}' AND MAX(r.revenue_date) < '{self.date_next}'
    """
    return sql

def process_tax_calculations():
    sql = """
    INSERT INTO finance.tax_records(order_id, tax_amount, tax_rate, tax_category, tax_date)
    SELECT 
        r.order_id,
        r.amount * 0.1 as tax_amount,
        10 as tax_rate,
        'sales_tax' as tax_category,
        r.revenue_date as tax_date
    FROM finance.revenue r
    WHERE r.revenue_date >= '{self.date}' AND r.revenue_date < '{self.date_next}'
    """
    return sql

def process_payment_methods_summary():
    sql = """
    INSERT INTO analytics.payment_method_summary(payment_method, transaction_count, total_amount, average_amount, month, year)
    SELECT 
        r.payment_method,
        COUNT(*) as transaction_count,
        SUM(r.amount) as total_amount,
        AVG(r.amount) as average_amount,
        MONTH(r.revenue_date) as month,
        YEAR(r.revenue_date) as year
    FROM finance.revenue r
    WHERE r.revenue_date >= '{self.date}' AND r.revenue_date < '{self.date_next}'
    GROUP BY r.payment_method, MONTH(r.revenue_date), YEAR(r.revenue_date)
    """
    return sql