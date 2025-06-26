import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Optional
#congfiure logging
logging.basicConfig(
    level = logging.INFO,
    format='%(asctime)s- %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityzChecker:
    """Handles data quality validation & reporting."""

    def __init__(self, engine):
        self.engine = engine


    def check_nulls(self, table_name: str) -> Dict[str, float]:
        """Calculate null percentage for each column in a table"""
        query = f"""
            SELECT 
                 column_name,
                 (COUNT(*)- COUNT(CASE WHEN{table_name}.* IS NOT NULL THEN 1 END)) * 100.0 COUNT(*) AS null_percentage
            FROM  {table_name}
            CROSS JOIN information_schema.columns
            WHERE table_name = '{table_name}'
            GROUP BY column_name
            """
        try:
            return pd.read_sql(query, self.engine).to_dict('records')
        except Exception as e:
            logger.error(f"Error checking nulls for {table_name}: {str(e)}")
            return {}
        

    def check_duplicates(self, table_name: str, key_columns: List[str]) -> int:
            """Check for duplicate records based on key columns"""
            key_cols = ", ".join(key_columns)
            query = f"""
                SELECT COUNT(*) - COUNT(DISTINCT) ({key_cols}) as duplicate_count
                FROM {table_name}
                 """
            
            try:
                return pd.read_sql(query, self.engine).iloc[0,0]
            except Exception as e:
                logger.error(f"Error checking duplicates for {table_name}: {str(e)}")
                return 0
            

    def validate_data_ranges(self, table_name: str, date_columns: str) -> Dict[str, datetime]:
            """Validate date ranges in a given column"""
            query= f"""
              SELECT 
                   MIN({date_columns}) as min_date,
                   MAX({date_columns}) as max_date
              FROM {table_name}

            """

            try:
                return pd.read_sql(query, self.engine).to_dict('records')[0]
            except Exception as e:
                logger.error(f"Error validating dates for {table_name}.{date_columns}: {str(e)}")
                return {}
            


