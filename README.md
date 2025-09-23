# google_ads_pipeline
This SQL cleans and aggregates raw Google Ads data from multiple tables

# Google Ads Data Cleaning Pipeline

## Overview
This SQL pipeline cleans and aggregates raw Google Ads data from multiple tables:
- Ad Group Stats
- Campaign Stats
- Keyword Stats

The pipeline outputs a consolidated table with:
- Date
- Campaign name & ID
- Ad Group name & ID
- Keyword text & ID
- Impressions, clicks, and spend (in standard currency)

## How it works
1. Filter out records with 0 impressions.
2. Consolidate ad group, campaign, and keyword stats.
3. Map IDs to human-readable names using reference tables.
4. Aggregate missing combinations with `UNION ALL`.
5. Output final cleaned dataset.

## Technology
- **SQL** (BigQuery)
- Can be integrated into ETL pipelines with Python or Airflow.
 

## Notes
- No real client data included; all examples use anonymized or synthetic data.
- Can be extended for daily automation in Airflow or Cloud Functions.
