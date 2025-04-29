def process_user_registration():
    sql = """
    INSERT INTO user_center.registered_users(user_id, username, email, phone, register_time, status)
    SELECT 
        id as user_id, 
        username, 
        email, 
        phone_number as phone, 
        create_time as register_time,
        1 as status
    FROM raw_data.user_signup_logs
    WHERE create_time >= '{self.date}' AND create_time < '{self.date_next}'
    """
    return sql

def process_user_profile():
    sql = """
    INSERT INTO user_center.user_profiles(user_id, nickname, avatar, gender, birthday, region)
    SELECT 
        user_id, 
        nickname, 
        avatar_url as avatar, 
        gender, 
        birthday, 
        region
    FROM raw_data.user_profile_updates
    WHERE update_time >= '{self.date}' AND update_time < '{self.date_next}'
    """
    return sql

def process_user_login():
    sql = """
    INSERT INTO user_center.login_history(user_id, login_time, ip_address, device_type, app_version)
    SELECT 
        user_id, 
        login_time, 
        ip_address, 
        device_type, 
        app_version
    FROM raw_data.login_logs
    WHERE login_time >= '{self.date}' AND login_time < '{self.date_next}'
    """
    return sql

def process_user_activity():
    sql = """
    INSERT INTO analytics.user_activity_summary(user_id, date, login_count, active_minutes, feature_usage)
    SELECT 
        user_id, 
        DATE(login_time) as date, 
        COUNT(*) as login_count, 
        SUM(session_duration)/60 as active_minutes,
        JSON_OBJECT('pages_visited', pages_visited, 'actions', actions) as feature_usage
    FROM raw_data.user_sessions
    WHERE login_time >= '{self.date}' AND login_time < '{self.date_next}'
    GROUP BY user_id, DATE(login_time)
    """
    return sql

def process_user_retention():
    sql = """
    INSERT INTO analytics.user_retention(cohort_date, days_since_signup, user_count, retention_rate)
    SELECT 
        DATE(u.register_time) as cohort_date,
        DATEDIFF(l.login_time, u.register_time) as days_since_signup,
        COUNT(DISTINCT u.user_id) as user_count,
        COUNT(DISTINCT u.user_id) / first_day.total_users as retention_rate
    FROM user_center.registered_users u
    JOIN user_center.login_history l ON u.user_id = l.user_id
    JOIN (
        SELECT 
            DATE(register_time) as signup_date, 
            COUNT(DISTINCT user_id) as total_users
        FROM user_center.registered_users
        GROUP BY DATE(register_time)
    ) first_day ON DATE(u.register_time) = first_day.signup_date
    WHERE u.register_time >= '{self.date}' AND u.register_time < '{self.date_next}'
    GROUP BY cohort_date, days_since_signup
    """
    return sql

def process_user_segments():
    sql = """
    INSERT INTO marketing.user_segments(user_id, segment_name, added_date, score)
    SELECT 
        u.user_id,
        CASE 
            WHEN a.active_minutes > 120 THEN 'power_user'
            WHEN a.active_minutes > 30 THEN 'active_user'
            ELSE 'casual_user'
        END as segment_name,
        CURRENT_DATE() as added_date,
        a.active_minutes / 10 as score
    FROM user_center.registered_users u
    JOIN analytics.user_activity_summary a ON u.user_id = a.user_id
    WHERE a.date >= '{self.date}' AND a.date < '{self.date_next}'
    """
    return sql

def process_user_preferences():
    sql = """
    INSERT INTO user_center.user_preferences(user_id, preference_key, preference_value, last_updated)
    SELECT 
        user_id,
        preference_name as preference_key,
        preference_value,
        update_time as last_updated
    FROM raw_data.user_settings
    WHERE update_time >= '{self.date}' AND update_time < '{self.date_next}'
    """
    return sql

def process_user_devices():
    sql = """
    INSERT INTO user_center.user_devices(user_id, device_id, device_name, platform, last_used)
    SELECT 
        user_id,
        device_id,
        device_name,
        os_type as platform,
        last_login_time as last_used
    FROM raw_data.device_registry
    WHERE register_time >= '{self.date}' AND register_time < '{self.date_next}'
    """
    return sql

def process_user_deletion():
    sql = """
    UPDATE user_center.registered_users
    SET status = 0, deletion_time = NOW()
    WHERE user_id IN (
        SELECT user_id 
        FROM raw_data.account_deletion_requests
        WHERE request_time >= '{self.date}' AND request_time < '{self.date_next}'
        AND status = 'approved'
    )
    """
    return sql