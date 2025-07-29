import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExtendedOlistAnalytics:
    
    def __init__(self, engine):
        self.engine = engine
        
    def customer_retention_analysis(self):
        query = """
        SELECT 
            c.customer_state,
            COUNT(DISTINCT c.customer_id) as total_customers,
            COUNT(DISTINCT CASE WHEN order_count > 1 THEN c.customer_id END) as repeat_customers,
            ROUND(COUNT(DISTINCT CASE WHEN order_count > 1 THEN c.customer_id END) * 100.0 / 
                  COUNT(DISTINCT c.customer_id)::NUMERIC, 2) as retention_rate,
            ROUND(AVG(order_count)::NUMERIC, 2) as avg_orders_per_customer,
            ROUND(AVG(total_spent)::NUMERIC, 2) as avg_customer_value
        FROM customers c
        JOIN (
            SELECT 
                customer_id,
                COUNT(order_id) as order_count,
                SUM(total_value) as total_spent
            FROM (
                SELECT 
                    o.customer_id,
                    o.order_id,
                    SUM(oi.price + oi.freight_value) as total_value
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.order_status = 'delivered'
                GROUP BY o.customer_id, o.order_id
            ) customer_orders
            GROUP BY customer_id
        ) customer_stats ON c.customer_id = customer_stats.customer_id
        GROUP BY c.customer_state
        ORDER BY retention_rate DESC
        """
        return pd.read_sql(query, self.engine)
    
    def payment_behavior_analysis(self):
        query = """
        SELECT 
            op.payment_type,
            op.payment_installments,
            COUNT(DISTINCT o.order_id) as order_count,
            ROUND(AVG(op.payment_value)::NUMERIC, 2) as avg_payment_value,
            ROUND(AVG(oi.price)::NUMERIC, 2) as avg_product_price,
            ROUND(AVG(r.review_score)::NUMERIC, 2) as avg_rating
        FROM order_payments op
        JOIN orders o ON op.order_id = o.order_id
        JOIN order_items oi ON o.order_id = oi.order_id
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY op.payment_type, op.payment_installments
        HAVING COUNT(DISTINCT o.order_id) > 100
        ORDER BY order_count DESC
        """
        return pd.read_sql(query, self.engine)

    def delivery_performance_analysis(self):
        query = """
        SELECT 
            s.seller_state,
            c.customer_state,
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(AVG(EXTRACT(DAYS FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)))::NUMERIC, 1) as avg_delivery_days,
            COUNT(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 END) as on_time_deliveries,
            ROUND(COUNT(CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 END) * 100.0 / 
                  COUNT(DISTINCT o.order_id)::NUMERIC, 2) as on_time_rate,
            ROUND(AVG(r.review_score)::NUMERIC, 2) as avg_rating
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN sellers s ON oi.seller_id = s.seller_id
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered' 
        AND o.order_delivered_customer_date IS NOT NULL
        GROUP BY s.seller_state, c.customer_state
        HAVING COUNT(DISTINCT o.order_id) > 50
        ORDER BY total_orders DESC
        """
        return pd.read_sql(query, self.engine)

    def seasonal_business_patterns(self):
        query = """
        SELECT 
            EXTRACT(YEAR FROM o.order_purchase_timestamp) as year,
            EXTRACT(MONTH FROM o.order_purchase_timestamp) as month,
            EXTRACT(DOW FROM o.order_purchase_timestamp) as day_of_week,
            COUNT(DISTINCT o.order_id) as order_count,
            ROUND(SUM(oi.price)::NUMERIC, 2) as total_revenue,
            ROUND(AVG(oi.price)::NUMERIC, 2) as avg_order_value,
            COUNT(DISTINCT o.customer_id) as unique_customers
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY year, month, day_of_week
        ORDER BY year, month, day_of_week
        """
        return pd.read_sql(query, self.engine)

    def top_products_analysis(self):
        query = """
        SELECT 
            p.product_id,
            p.product_category_name,
            COUNT(DISTINCT oi.order_id) as total_orders,
            ROUND(SUM(oi.price)::NUMERIC, 2) as total_revenue,
            ROUND(AVG(oi.price)::NUMERIC, 2) as avg_price,
            ROUND(AVG(r.review_score)::NUMERIC, 2) as avg_rating,
            COUNT(r.review_id) as review_count,
            RANK() OVER (ORDER BY SUM(oi.price) DESC) as revenue_rank
        FROM products p
        JOIN order_items oi ON p.product_id = oi.product_id
        JOIN orders o ON oi.order_id = o.order_id
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY p.product_id, p.product_category_name
        HAVING COUNT(DISTINCT oi.order_id) > 20
        ORDER BY total_revenue DESC
        LIMIT 100
        """
        return pd.read_sql(query, self.engine)

    def customer_geography_insights(self):
        query = """
        SELECT 
            c.customer_state,
            c.customer_city,
            COUNT(DISTINCT c.customer_id) as customer_count,
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) as total_gmv,
            ROUND(AVG(oi.price + oi.freight_value)::NUMERIC, 2) as avg_order_value,
            ROUND(AVG(r.review_score)::NUMERIC, 2) as avg_rating
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_state, c.customer_city
        HAVING COUNT(DISTINCT o.order_id) > 30
        ORDER BY total_gmv DESC
        """
        return pd.read_sql(query, self.engine)

    def order_value_distribution(self):
        query = """
        SELECT 
            CASE 
                WHEN order_value < 50 THEN 'Low (< $50)'
                WHEN order_value < 150 THEN 'Medium ($50-150)'
                WHEN order_value < 300 THEN 'High ($150-300)'
                ELSE 'Premium (> $300)'
            END as order_value_segment,
            COUNT(DISTINCT order_id) as order_count,
            ROUND(AVG(order_value)::NUMERIC, 2) as avg_order_value,
            ROUND(SUM(order_value)::NUMERIC, 2) as total_revenue,
            ROUND(AVG(avg_rating)::NUMERIC, 2) as avg_segment_rating
        FROM (
            SELECT 
                o.order_id,
                SUM(oi.price + oi.freight_value) as order_value,
                AVG(r.review_score) as avg_rating
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN order_reviews r ON o.order_id = r.order_id
            WHERE o.order_status = 'delivered'
            GROUP BY o.order_id
        ) order_values
        GROUP BY order_value_segment
        ORDER BY avg_order_value
        """
        return pd.read_sql(query, self.engine)

    def seller_concentration_analysis(self):
        query = """
        SELECT 
            s.seller_state,
            COUNT(DISTINCT s.seller_id) as seller_count,
            COUNT(DISTINCT oi.order_id) as total_orders,
            ROUND(SUM(oi.price)::NUMERIC, 2) as total_revenue,
            ROUND(AVG(oi.price)::NUMERIC, 2) as avg_seller_performance,
            ROUND(AVG(r.review_score)::NUMERIC, 2) as avg_rating,
            COUNT(DISTINCT oi.product_id) as unique_products_sold
        FROM sellers s
        JOIN order_items oi ON s.seller_id = oi.seller_id
        JOIN orders o ON oi.order_id = o.order_id
        LEFT JOIN order_reviews r ON o.order_id = r.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY s.seller_state
        ORDER BY total_revenue DESC
        """
        return pd.read_sql(query, self.engine)

    def export_tableau_datasets(self, output_dir='tableau_data'):
        os.makedirs(output_dir, exist_ok=True)
        
        datasets = {
            'customer_retention': self.customer_retention_analysis(),
            'payment_behavior': self.payment_behavior_analysis(),
            'delivery_performance': self.delivery_performance_analysis(),
            'seasonal_patterns': self.seasonal_business_patterns(),
            'top_products': self.top_products_analysis(),
            'customer_geography': self.customer_geography_insights(),
            'order_value_distribution': self.order_value_distribution(),
            'seller_concentration': self.seller_concentration_analysis()
        }
        
        for name, df in datasets.items():
            if not df.empty:
                df.to_csv(f'{output_dir}/{name}.csv', index=False)
                logger.info(f"Exported {name}.csv with {len(df)} rows")
            else:
                logger.warning(f"No data found for {name}")
        
        return datasets


def main():
    try:
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")

        if not db_url:
            logger.error("DATABASE_URL not found in environment variables")
            return

        engine = create_engine(db_url)
        analytics = ExtendedOlistAnalytics(engine)

        logger.info("Starting extended analytics and Tableau data export...")
        datasets = analytics.export_tableau_datasets()

        print("\n" + "="*50)
        print("EXTENDED OLIST ANALYTICS SUMMARY")
        print("="*50)

        retention_data = datasets['customer_retention']
        if not retention_data.empty:
            print(f"\nCustomer Retention Analysis:")
            print(f"- Best retention state: {retention_data.iloc[0]['customer_state']} ({retention_data.iloc[0]['retention_rate']:.1f}%)")
            print(f"- Average retention rate: {retention_data['retention_rate'].mean():.1f}%")

        payment_data = datasets['payment_behavior']
        if not payment_data.empty:
            top_payment = payment_data.iloc[0]
            print(f"\nPayment Behavior:")
            print(f"- Most popular: {top_payment['payment_type']} ({top_payment['order_count']:,} orders)")
            print(f"- Average payment value: ${payment_data['avg_payment_value'].mean():.2f}")

        delivery_data = datasets['delivery_performance']
        if not delivery_data.empty:
            print(f"\nDelivery Performance:")
            print(f"- Average delivery time: {delivery_data['avg_delivery_days'].mean():.1f} days")
            print(f"- Average on-time rate: {delivery_data['on_time_rate'].mean():.1f}%")

        products_data = datasets['top_products']
        if not products_data.empty:
            print(f"\nTop Products:")
            print(f"- Highest revenue product: {products_data.iloc[0]['product_category_name']}")
            print(f"- Revenue: ${products_data.iloc[0]['total_revenue']:,.2f}")

        print(f"\n✅ All datasets exported to 'tableau_data' folder")
        print(f"✅ Ready for Tableau dashboard creation!")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()
