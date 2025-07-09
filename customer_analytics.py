import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

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
     """