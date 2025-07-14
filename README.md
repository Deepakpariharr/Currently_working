# Currently_working
# Analysis of E-Commerce Public Dataset by Olist

This project provides a complete end-to-end business analysis pipeline using SQL, Python, and Tableau. The core aim is to uncover customer behaviors, seller trends, product lifecycle stages, and operational insights using the Olist e-commerce dataset.

The complete walkthrough and Tableau dashboard setup guide are included in this repository.

---

## Motivation

This project aims to address common real-world challenges faced by e-commerce businesses. The focus is on:

- Understanding customer value using RFM segmentation
- Analyzing product performance and lifecycle patterns
- Evaluating seller delivery efficiency and customer satisfaction
- Identifying trends based on geography, payment methods, and seasonality
- Providing Tableau dashboards for business decision-makers

All outputs are exported as CSVs, which are then used to create interactive dashboards in Tableau for further exploration and stakeholder reporting.

---

## Dataset

This project uses the **Brazilian e-commerce public dataset by Olist**, which contains over 100,000 orders made between 2016 and 2018 across multiple marketplaces in Brazil.

It includes:
- Order history
- Customer and seller location data
- Product details
- Payment methods and values
- Shipping and delivery information
- Customer reviews

The dataset can be found here:  
**[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)**

---

## Files Description

This is the file-folder structure of the project:

├── analytics/
│ ├── extended_analytics.py # Tableau export logic (multiple domain-specific exports)
│ └── core_analytics.py # RFM, product lifecycle, seller metrics
│
├── tableau_data/ # Output data for Tableau dashboards
│ ├── customer_retention.csv
│ ├── payment_behavior.csv
│ ├── delivery_performance.csv
│ ├── seasonal_patterns.csv
│ ├── top_products.csv
│ ├── customer_geography.csv
│ ├── order_value_distribution.csv
│ └── seller_concentration.csv
│
├── dashboard_data/ # Core analytics output
│
├── requirements.txt # Python dependencies
└── README.md


---

## Main Features

- **Customer Segmentation** using RFM metrics and state-wise breakdown
- **Product Lifecycle Analysis** including revenue growth and seasonality
- **Seller Performance Insights** with delivery times, review scores, and freight costs
- **Geographical Analysis** of buyer-seller locations and on-time delivery rate
- **Payment Behavior** including types, value per order, and installments
- **Seasonal & Temporal Trends** broken down by hour, weekday, and month
- **Cohort Analysis** for long-term retention tracking

All outputs are designed to be directly consumed by Tableau.



