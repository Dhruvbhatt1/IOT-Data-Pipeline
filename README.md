# IoT Data Pipeline Project

## Overview
This project demonstrates an **IoT Data Pipeline** built on **Azure Cloud** to process, analyze, and visualize IoT data. The pipeline utilizes **Medallion Architecture** (Bronze, Silver, Gold layers) to organize and refine the data efficiently, ensuring scalability and reliability. Python was used to simulate IoT data.

---

## Table of Contents
1. [Features](#features)  
2. [Tech Stack](#tech-stack)  
3. [Architecture](#architecture)  
4. [Medallion Architecture](#medallion-architecture)  
5. [Setup and Execution](#setup-and-execution)  
6. [Future Enhancements](#future-enhancements)  

---

## Features
- **Data Ingestion**: IoT data simulated using Python and ingested via **Azure Event Hub**.
- **Data Transformation**: Data refined using **Azure Databricks** (PySpark).
- **Data Storage**: Organized in **Azure Data Lake Storage Gen2 (ADLS Gen2)** using Medallion Architecture.
- **ETL Pipelines**: Built and scheduled with **Azure Data Factory (ADF)**.
- **SQL Analysis**: Transformed data queried using **SQL** for downstream processing.
- **Scalable Design**: Supports large-scale IoT datasets with efficient processing.

---

## Tech Stack
- **Azure Services**:  
  - Azure Data Factory (ADF)  
  - Azure Data Lake Storage Gen2 (ADLS Gen2)  
  - Azure Databricks  
  - Azure Event Hub  
- **Programming and Frameworks**:  
  - Python  
  - PySpark  
  - SQL

---

## Architecture
The pipeline follows a structured design to ensure end-to-end data processing:

1. **Data Generation**: IoT data simulated using Python and streamed to **Azure Event Hub**.
2. **Data Ingestion**: ADF pipelines ingest data from Event Hub into the **Bronze Layer** of ADLS Gen2.
3. **Data Transformation**: PySpark in Azure Databricks processes the data in the Bronze Layer, applies schema validation and cleaning, and writes refined data to the **Silver Layer**.
4. **Data Aggregation**: Further transformations and aggregations are performed, with the results stored in the **Gold Layer**.
5. **Data Consumption**: Processed data in the Gold Layer is used for reporting and analysis via SQL.

---

## Medallion Architecture
This project adheres to the **Medallion Architecture**, a modular approach to organizing data:

- **Bronze Layer**: Stores raw, unprocessed IoT data as ingested from the Event Hub.  
- **Silver Layer**: Contains cleaned and enriched data, suitable for intermediate analysis.  
- **Gold Layer**: Stores aggregated and highly structured data for business intelligence and reporting.

---

## Setup and Execution

### Prerequisites
- Active **Azure Subscription**.
- Azure Event Hub, Data Factory, Data Lake Gen2, and Databricks configured.
- Python environment with necessary libraries installed (for data simulation).

### Steps
1. **Simulate IoT Data**:
   - Run the provided Python script to simulate IoT data and stream it to Azure Event Hub.

2. **Create ADF Pipelines**:
   - Configure ADF pipelines to ingest data from Event Hub into ADLS Gen2 (Bronze Layer).

3. **Process Data with Azure Databricks**:
   - Use the provided PySpark notebooks to clean and transform the data.
   - Save the results to the Silver and Gold Layers.

4. **Query Data**:
   - Use SQL to query the data in the Gold Layer for insights and reporting.

### Example Commands
- **Run Data Simulation**:  
  ```bash
  python iot_data_simulator.py
  ```
- **Start ADF Pipeline**:  
  Trigger the pipeline from the Azure Portal or using Azure CLI.

---

## Future Enhancements
- **Real-Time Dashboards**: Integrate with Power BI for live IoT data visualization.
- **Machine Learning**: Implement predictive analytics for IoT sensor data.
- **Automation**: Automate infrastructure setup using Terraform or ARM templates.

---

**Contributions**: Contributions and suggestions are welcome! Feel free to open issues or submit pull requests.

**License**: [MIT](LICENSE)

---
