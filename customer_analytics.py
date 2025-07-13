from matplotlib.backend_bases import cursors
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:deepak@localhost/')

class OlistCustomerAnalytics:
    def __init__(self, db_connection):
        self.db = db_connection

    def brazilian_rfm_analysis(self):
        """
        RFM analysis specifically tailored for Brazillian e-commerec patterns
        """

        query = """
        SELECT 
     c.customer_id,
     c.customer_state,
     c.customer_city,
     MAX(o.order_purchase_timestamp) as last_purchase,
     COUNT(DITINCT o.order_id) as frequency,
     SUM(p.payment_value) as monetary_value,
     AVG(p.payment_value) as avg_order_value,
     COUNT(DISTINCT i.product_id) as product_diversity,
     AVG(EXTRACTION(DAYS  FROM (o.order_delivered_customer_date - o.order_purchase_timestamp))) as avg_delivery_days
     FROM customers_dataset c
     JOIN order_dataset o ON c.customer_id = o.customer_id
     JOIN order_payments_dataset p ON o.order_id = p.order_id
     JOIN order_items_dataset i ON o.order_id = i.order_id
     WHERE o.order_status = 'deliverd'
     GROUP BY c.customer_id, c.customer_state, c.customer_city
     """
        df = pd.read_sql(query, self.db)
        #Calculalting recency considering Brazilian holiday seasons
        reference_date = pd.to_datetime('2018-10-17')
        df['recency'] = (reference_date - pd.to_datetime(df['last_purchase'])).dt

        #Brazilian-specific RFM scoring considering regional differences
        df = self._calculate_regional_rfm_scores(df)
        df['customer_segment'] = self.__assign_brazilian_segments(df)ddd       

        return df
    

    def regional_customer_analysis(self):
        """
        Analysing customer Behaviour by Brazilian states and regions
        """
        query = """
        CREATE TABLE IF NOT EXISTS state_region_mapping AS 
        SELECT 
            geolocation_state AS state_code
            CASE 
              WHEN lat < -25 THEN 'South'
              WHEN lat BETWEEN -25 AND -15 AND lng > -50 THEN  'Southeast'
              WHEN lat BETWEEN -15 AND -5 THEN 'Northest'
              WHEN lat > -5 THEN 'North'
              ELSE 'Center-West'
            END AS region
        FROM (
           SELECT
               gelocation_state,
               AVG(geolocation_lat) AS lat,
               AVG(geolocation_lng) AS lng
           FROM olist_gelocation_dataset
        ) sub;
        """
        
        return pd.read_sql(query, self.db)
    
    def brzilian_holiday_impact_analysis(self):
        """
        Analyzing the sales patterns during Brazilian holidays and special dates"""

        brazilian_holidays = {
            'Black Friday': ['2017-11-24', '2018-11-23'],
            'Christmas': ['2017-12-25', '2018-12-25'],
            'Mothers Day': ['2017-05-14', '2018-05-13'],
            'Fathers Day': ['2017-08-13', '2018-08-12'],
            'Valentines Day': ['2017-06-12', '2018-06-12'],  
            'Carnival': ['2017-02-27', '2018-02-13']
        }
        query = """
        SELECT 
            DATE(o.order_purchase_timestamp) as order_date,
            COUNT(DISTINCT o.order_id) as daily_orders,
            SUM(p.payment_value) as daily_revenue,
            AVG(p.payment_value) as avg_order_value
        FROM orders_dataset o
        JOIN order_payment_dataset p ON o ON o.order_id = p.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY DATE(o.order_purchase_timestamp)
        ORDER BY order_date"""

        daily_sales = pd.read_sql(query, self.db)
        daily_sales['order_date'] = pd.to_datetime(daily_sales['order_date'])

        #adding holyday flags
        for holiday, dates in brazilian_holidays.items():
            holiyday_dates = pd.to_date(dates)
            daily_sales[f'{holiday.lower()}_period'] = daily_sales['order_date'].apply(
                lambda x: any(abs((x - hdate).days) <= 7 for hdate in holiyday_dates)
            )

        return daily_sales
    
    
