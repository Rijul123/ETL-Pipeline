

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AnalyticsReporter:
    """Generate business intelligence reports from the data warehouse"""
    
    def __init__(self, engine):
        self.engine = engine
    
    def execute_query(self, query, params=None):
        """Execute SQL query and return DataFrame"""
        try:
            return pd.read_sql(query, self.engine, params=params)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    
    def get_top_selling_products(self, limit=10):
        """
        Get top selling products by revenue
        
        Args:
            limit: Number of products to return
            
        Returns:
            DataFrame with top products
        """
        query = """
        SELECT 
            p.product_name,
            p.category,
            SUM(fs.quantity) as total_quantity_sold,
            SUM(fs.revenue) as total_revenue,
            SUM(fs.profit) as total_profit,
            ROUND(AVG(fs.unit_price), 2) as avg_selling_price
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        GROUP BY p.product_key, p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT %(limit)s
        """
        return self.execute_query(query, {'limit': limit})
    
    def get_revenue_by_region(self):
        """Get revenue breakdown by region"""
        query = """
        SELECT 
            s.region,
            COUNT(DISTINCT fs.sale_id) as total_transactions,
            SUM(fs.quantity) as total_units_sold,
            ROUND(SUM(fs.revenue), 2) as total_revenue,
            ROUND(SUM(fs.profit), 2) as total_profit,
            ROUND(AVG(fs.revenue), 2) as avg_transaction_value
        FROM fact_sales fs
        JOIN dim_store s ON fs.store_key = s.store_key
        GROUP BY s.region
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)
    
    def get_monthly_sales_trends(self):
        """Get monthly sales trends"""
        query = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(*) as total_transactions,
            SUM(fs.quantity) as total_quantity,
            ROUND(SUM(fs.revenue), 2) as total_revenue,
            ROUND(SUM(fs.profit), 2) as total_profit,
            ROUND(AVG(fs.revenue), 2) as avg_revenue_per_transaction
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year DESC, d.month DESC
        """
        return self.execute_query(query)
    
    def get_sales_by_store(self):
        """Get sales performance by store"""
        query = """
        SELECT 
            s.store_name,
            s.city,
            s.state,
            s.region,
            COUNT(*) as total_transactions,
            SUM(fs.quantity) as total_units_sold,
            ROUND(SUM(fs.revenue), 2) as total_revenue,
            ROUND(SUM(fs.profit), 2) as total_profit,
            ROUND(AVG(fs.revenue), 2) as avg_transaction_value
        FROM fact_sales fs
        JOIN dim_store s ON fs.store_key = s.store_key
        GROUP BY s.store_key, s.store_name, s.city, s.state, s.region
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)
    
    
    def get_customer_purchasing_patterns(self):
        """Analyze customer purchasing patterns"""
        query = """
        SELECT 
            c.customer_segment,
            COUNT(DISTINCT c.customer_key) as customer_count,
            COUNT(fs.sale_id) as total_orders,
            ROUND(AVG(order_stats.orders_per_customer), 2) as avg_orders_per_customer,
            ROUND(SUM(fs.revenue), 2) as total_revenue,
            ROUND(AVG(order_stats.avg_order_value), 2) as avg_order_value
        FROM fact_sales fs
        JOIN dim_customer c ON fs.customer_key = c.customer_key
        JOIN (
            SELECT 
                customer_key,
                COUNT(*) as orders_per_customer,
                AVG(revenue) as avg_order_value
            FROM fact_sales
            GROUP BY customer_key
        ) order_stats ON fs.customer_key = order_stats.customer_key
        GROUP BY c.customer_segment
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)
    
    def get_top_customers(self, limit=10):
        """Get top customers by revenue"""
        query = """
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            c.email,
            c.city,
            c.state,
            c.customer_segment,
            COUNT(fs.sale_id) as total_orders,
            SUM(fs.quantity) as total_items_purchased,
            ROUND(SUM(fs.revenue), 2) as total_spent,
            ROUND(AVG(fs.revenue), 2) as avg_order_value
        FROM fact_sales fs
        JOIN dim_customer c ON fs.customer_key = c.customer_key
        GROUP BY c.customer_key, c.first_name, c.last_name, c.email, 
                 c.city, c.state, c.customer_segment
        ORDER BY total_spent DESC
        LIMIT %(limit)s
        """
        return self.execute_query(query, {'limit': limit})
    

    
    def get_category_performance(self):
        """Get performance by product category"""
        query = """
        SELECT 
            p.category,
            COUNT(DISTINCT p.product_key) as product_count,
            COUNT(fs.sale_id) as total_sales,
            SUM(fs.quantity) as total_quantity_sold,
            ROUND(SUM(fs.revenue), 2) as total_revenue,
            ROUND(SUM(fs.profit), 2) as total_profit,
            ROUND(AVG(fs.profit), 2) as avg_profit_per_sale,
            ROUND(SUM(fs.profit) / NULLIF(SUM(fs.revenue), 0) * 100, 2) as profit_margin_pct
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        GROUP BY p.category
        ORDER BY total_revenue DESC
        """
        return self.execute_query(query)
    
    def get_daily_sales_summary(self, days=30):
        """Get daily sales summary for recent days"""
        query = """
        SELECT 
            d.full_date,
            d.day_name,
            COUNT(*) as transactions,
            SUM(fs.quantity) as units_sold,
            ROUND(SUM(fs.revenue), 2) as revenue,
            ROUND(SUM(fs.profit), 2) as profit
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '%(days)s days'
        GROUP BY d.full_date, d.day_name
        ORDER BY d.full_date DESC
        """
        return self.execute_query(query, {'days': days})
    
    # ============================================================
    # EXECUTIVE DASHBOARD
    # ============================================================
    
    def get_executive_summary(self):
        """Get executive summary KPIs"""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM fact_sales) as total_transactions,
            (SELECT SUM(revenue) FROM fact_sales) as total_revenue,
            (SELECT SUM(profit) FROM fact_sales) as total_profit,
            (SELECT SUM(quantity) FROM fact_sales) as total_units_sold,
            (SELECT COUNT(DISTINCT customer_key) FROM fact_sales) as unique_customers,
            (SELECT COUNT(DISTINCT product_key) FROM fact_sales) as products_sold,
            (SELECT ROUND(AVG(revenue), 2) FROM fact_sales) as avg_transaction_value,
            (SELECT ROUND(SUM(profit) / NULLIF(SUM(revenue), 0) * 100, 2) FROM fact_sales) as overall_profit_margin
        """
        return self.execute_query(query)
    
    def generate_full_report(self):
        """Generate a comprehensive analytics report"""
        logger.info("=" * 60)
        logger.info("GENERATING ANALYTICS REPORT")
        logger.info("=" * 60)
        
        report = {
            'executive_summary': self.get_executive_summary(),
            'top_products': self.get_top_selling_products(10),
            'revenue_by_region': self.get_revenue_by_region(),
            'monthly_trends': self.get_monthly_sales_trends(),
            'store_performance': self.get_sales_by_store(),
            'customer_patterns': self.get_customer_purchasing_patterns(),
            'top_customers': self.get_top_customers(10),
            'category_performance': self.get_category_performance(),
            'daily_summary': self.get_daily_sales_summary(30)
        }
        
        logger.info("✅ Analytics report generated successfully")
        return report


# SQL Queries for reference
QUERIES = {
    'top_products': """
        SELECT p.product_name, SUM(fs.revenue) as total_revenue
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY total_revenue DESC
        LIMIT 10
    """,
    
    'revenue_by_region': """
        SELECT s.region, SUM(fs.revenue) as total_revenue
        FROM fact_sales fs
        JOIN dim_store s ON fs.store_key = s.store_key
        GROUP BY s.region
        ORDER BY total_revenue DESC
    """,
    
    'monthly_trends': """
        SELECT d.year, d.month, SUM(fs.revenue) as total_revenue
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year DESC, d.month DESC
    """,
    
    'customer_insights': """
        SELECT c.customer_segment, 
               COUNT(DISTINCT c.customer_key) as customer_count,
               SUM(fs.revenue) as total_revenue
        FROM fact_sales fs
        JOIN dim_customer c ON fs.customer_key = c.customer_key
        GROUP BY c.customer_segment
    """
}


if __name__ == "__main__":
    # Test analytics
    logging.basicConfig(level=logging.INFO)
    
    import sys
    sys.path.append('../warehouse')
    from db_config import create_db_engine
    
    engine = create_db_engine()
    reporter = AnalyticsReporter(engine)
    
    # Generate and display reports
    print("\n" + "=" * 60)
    print("EXECUTIVE SUMMARY")
    print("=" * 60)
    print(reporter.get_executive_summary())
    
    print("\n" + "=" * 60)
    print("TOP 10 PRODUCTS")
    print("=" * 60)
    print(reporter.get_top_selling_products(10))
    
    print("\n" + "=" * 60)
    print("REVENUE BY REGION")
    print("=" * 60)
    print(reporter.get_revenue_by_region())
