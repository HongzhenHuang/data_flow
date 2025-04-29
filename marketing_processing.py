def process_campaign_creation():
    sql = """
    INSERT INTO marketing.campaigns(campaign_id, name, description, start_date, end_date, budget, status)
    SELECT 
        campaign_id, 
        campaign_name as name, 
        campaign_description as description, 
        start_date, 
        end_date, 
        budget_amount as budget, 
        campaign_status as status
    FROM raw_data.marketing_campaigns
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_campaign_channels():
    sql = """
    INSERT INTO marketing.campaign_channels(campaign_id, channel, budget_allocation, start_date, end_date)
    SELECT 
        campaign_id, 
        channel_name as channel, 
        allocated_budget as budget_allocation, 
        channel_start_date as start_date, 
        channel_end_date as end_date
    FROM raw_data.campaign_channels
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_promotion_codes():
    sql = """
    INSERT INTO marketing.promotion_codes(code, campaign_id, discount_type, discount_value, start_date, end_date, usage_limit)
    SELECT 
        promo_code as code, 
        campaign_id, 
        discount_type, 
        discount_amount as discount_value, 
        valid_from as start_date, 
        valid_to as end_date, 
        max_uses as usage_limit
    FROM raw_data.promotion_codes
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_promotion_usage():
    sql = """
    INSERT INTO marketing.promotion_usage(code, order_id, user_id, usage_time, discount_amount)
    SELECT 
        promo_code as code, 
        order_id, 
        user_id, 
        apply_time as usage_time, 
        discount_amount
    FROM raw_data.promotion_usage_logs
    WHERE apply_time >= '{self.date}' AND apply_time < '{self.date_next}'
    """
    return sql

def process_email_campaigns():
    sql = """
    INSERT INTO marketing.email_campaigns(campaign_id, subject, content, sent_time, recipient_count)
    SELECT 
        campaign_id, 
        email_subject as subject, 
        email_content as content, 
        send_time as sent_time, 
        recipient_count
    FROM raw_data.email_campaigns
    WHERE send_time >= '{self.date}' AND send_time < '{self.date_next}'
    """
    return sql

def process_email_metrics():
    sql = """
    INSERT INTO analytics.email_performance(campaign_id, date, emails_sent, open_count, click_count, open_rate, click_rate)
    SELECT 
        e.campaign_id, 
        DATE(e.sent_time) as date, 
        e.recipient_count as emails_sent, 
        COUNT(DISTINCT CASE WHEN m.event_type = 'open' THEN m.user_id END) as open_count,
        COUNT(DISTINCT CASE WHEN m.event_type = 'click' THEN m.user_id END) as click_count,
        COUNT(DISTINCT CASE WHEN m.event_type = 'open' THEN m.user_id END) / e.recipient_count as open_rate,
        COUNT(DISTINCT CASE WHEN m.event_type = 'click' THEN m.user_id END) / e.recipient_count as click_rate
    FROM marketing.email_campaigns e
    LEFT JOIN raw_data.email_events m ON e.campaign_id = m.campaign_id
    WHERE e.sent_time >= '{self.date}' AND e.sent_time < '{self.date_next}'
    GROUP BY e.campaign_id, DATE(e.sent_time)
    """
    return sql

def process_campaign_performance():
    sql = """
    INSERT INTO analytics.campaign_performance(campaign_id, date, impressions, clicks, conversions, spend, revenue, roi)
    SELECT 
        c.campaign_id, 
        DATE(m.event_date) as date, 
        SUM(CASE WHEN m.event_type = 'impression' THEN m.event_count ELSE 0 END) as impressions,
        SUM(CASE WHEN m.event_type = 'click' THEN m.event_count ELSE 0 END) as clicks,
        COUNT(DISTINCT o.order_id) as conversions,
        SUM(m.cost) as spend,
        SUM(o.total_amount) as revenue,
        CASE WHEN SUM(m.cost) > 0 THEN SUM(o.total_amount) / SUM(m.cost) ELSE 0 END as roi
    FROM marketing.campaigns c
    JOIN raw_data.marketing_metrics m ON c.campaign_id = m.campaign_id
    LEFT JOIN order_center.orders o ON m.user_id = o.user_id AND DATE(m.event_date) = DATE(o.order_time)
    WHERE m.event_date >= '{self.date}' AND m.event_date < '{self.date_next}'
    GROUP BY c.campaign_id, DATE(m.event_date)
    """
    return sql

def process_user_acquisition():
    sql = """
    INSERT INTO analytics.user_acquisition(date, source, new_users, conversion_rate, acquisition_cost)
    SELECT 
        DATE(u.register_time) as date,
        u.referral_source as source,
        COUNT(DISTINCT u.user_id) as new_users,
        COUNT(DISTINCT o.order_id) / COUNT(DISTINCT u.user_id) as conversion_rate,
        SUM(m.cost) / COUNT(DISTINCT u.user_id) as acquisition_cost
    FROM user_center.registered_users u
    LEFT JOIN order_center.orders o ON u.user_id = o.user_id AND DATE(u.register_time) = DATE(o.order_time)
    LEFT JOIN raw_data.marketing_metrics m ON u.referral_source = m.source AND DATE(u.register_time) = DATE(m.event_date)
    WHERE u.register_time >= '{self.date}' AND u.register_time < '{self.date_next}'
    GROUP BY DATE(u.register_time), u.referral_source
    """
    return sql

def update_campaign_status():
    sql = """
    UPDATE marketing.campaigns
    SET status = 'completed'
    WHERE end_date < CURRENT_DATE()
    AND status = 'active'
    """
    return sql