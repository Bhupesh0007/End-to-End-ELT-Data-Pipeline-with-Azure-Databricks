# End-to-End ELT Data Pipeline with Azure Databricks

![image](https://raw.githubusercontent.com/Bhupesh0007/spotify_etl_pipeline/refs/heads/main/spotify_pipeline_architecture_diagram.webp)

## 🚀 Introduction

This project implements a modern ELT data pipeline using Azure Databricks and the Medallion Architecture. It processes raw Parquet files from Azure Data Lake Storage (ADLS), refines them through Bronze, Silver, and Gold layers, and creates business-ready dimensional models suitable for analytics.

## 🤔 Why Medallion Architecture?

The Medallion (or multi-hop) architecture is a design pattern that logically organizes data in a lakehouse. It progressively improves the structure and quality of data as it moves through each layer. The primary benefits include:

  * **Improved Data Reliability:** By staging data in Bronze, Silver, and Gold layers, we can build robust, incremental ETL processes that are easier to debug. If a job fails, we can often restart from the previous layer instead of re-ingesting the raw data.
  * **Enhanced Data Quality:** Data is validated and cleaned as it moves from Bronze to Silver, and business rules are applied before it reaches the Gold layer. This ensures that business analysts and data scientists are working with the highest quality data. For example, the product dimension table has data quality expectations to ensure product IDs and names are not null.
  * **Supports Diverse Use Cases:** The different layers can serve various users and purposes. Data scientists might explore the raw data in the Bronze layer, while business analysts will use the highly-structured, aggregated data in the Gold layer for their reports and dashboards.
  * **Simplified Governance:** The architecture provides a clear data lineage, making it easier to track transformations and manage data access policies for each layer.

## ✨ Key Features & Technology Stack

  * **Architecture:** Medallion (Bronze, Silver, Gold) on Delta Lake.
  * **Platform:** Azure Databricks, Apache Spark.
  * **Storage:** Azure Data Lake Storage (ADLS) Gen2.
  * **Key Techniques:**
      * Slowly Changing Dimensions (SCD Type 1 for Customers, SCD Type 2 for Products).
      * Incremental data ingestion using Auto Loader (`cloudFiles`).
      * Dynamic, parameterized notebooks for reusability.
      * Window functions, UDFs, and schema enforcement.

## 🏗️ Pipeline Overview

1.  **🥉 Bronze Layer (Raw Ingestion):**

      * A dynamic streaming notebook ingests raw Parquet files from ADLS into Bronze Delta tables without modification. This creates a complete and raw historical archive.

2.  **🥈 Silver Layer (Cleansed & Conformed):**

      * Data from the Bronze layer is cleaned, transformed, and enriched.
      * **Customers:** Names are concatenated and email domains are extracted.
      * **Orders:** Data types are corrected and window functions are applied to rank orders.
      * **Products:** UDFs are used to calculate a discounted price and standardize brand names.

3.  **🥇 Gold Layer (Business-Ready):**

      * The Silver data is modeled into dimension and fact tables for analytics.
      * **`DimCustomer`:** A customer dimension table that implements SCD Type 1, where existing records are overwritten with the latest data.
      * **`DimProducts`:** A product dimension table managed by a Delta Live Tables (DLT) pipeline, which tracks historical changes using SCD Type 2.
      * **`FactOrders`:** A fact table created by joining orders with the `DimCustomer` and `DimProduct` dimension keys.

## 🔧 How to Run the Project

1.  **Setup:**

      * Ensure you have an Azure Databricks workspace and an ADLS Gen2 storage account with `source`, `bronze`, `silver`, and `gold` containers.
      * Upload the source Parquet files to the `/source` container.
      * Import all `.py` notebooks into your workspace.

2.  **Execution:**

      * Run the notebooks in logical order, starting with the `Bronze_Layer.py` notebook for each data source.
      * Proceed to run the `Silver_*` notebooks to clean the data.
      * Finally, run the `Gold_*` notebooks to create the final analytical tables. Note that `Gold_Products.py` must be run as a Delta Live Tables pipeline.
