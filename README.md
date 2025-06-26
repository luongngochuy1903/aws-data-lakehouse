# Building data lakehouse with etl pipelines on cloud with AWS
![ETL](https://img.shields.io/badge/Type-ETL,lakehouse-blue)
Data engineer project builds the overall look of Cloud-based **data lakehouse architecture** combining scalable power of **data lake** and query performance with schema of a **data warehouse**
## CONTENT
- [System Architecture](#system-architecture)
- [Workflow](#workflow)
- [Result](#result)
## System Architecture
![architecture](https://github.com/user-attachments/assets/8f94535a-ce68-42fd-ae21-7a8046b1c5ec)
## 🚀Workflow
**Data source**  
Coming from various kind of input flow like streaming, putting, API, back-end request. S3 plays a role of input storage file which can be triggered by any event no matter of scheduling.  
**Lakehouse workflow**  
- When a signal of input appears which is detected by **AWS Lambda Trigger** implemening on **S3 Bucket** *input*.
- Using **AWS glue table catalog** as the **S3 Bucket** address. That means we are using "frame" table with only metadata of data source existing in **S3 Bucket** to execute any transformation or aggregation logic instead of directly implementing to **S3 Bucket**.
**ETL**
- Using dynamic frames as target object to avoid nested or complicated schema confliction. Although we still apply all the transformation to dataframes using pyspark.
- ETL pipeline runs from *Bronze layer* to *Gold layer* through stages, achieves requirement of each stage:
  - In 🟫**Bronze layer**, the raw version of datasets is put in priority with a view to acquire business flexibility or data quality.
  - In 🟪**Silver layer**, considering whole null or meaningless, unreliable values to be eliminated. Some changing of column's name should be considered too. This status of data here should be available for machine learning, model learning or other AI problems.
  - In 🟨**Gold layer**, reconstructing schema management with business standard, guaranteeing data type and doing some aggregation. Table in this layer must be perfectly completed by all database standards, which **Apache Iceberg table format** is a great scene. 💡*Gold layer* is now playing a role as **Data warehouse** inside a *Data lake*.
- Finally analyzing can be conducted with **AWS Athena**.
## Result
- As the result we absorb all raw data from input source and storing data in a **3-layer hierarchy** architecture, each layer has major impacts for other using data users or end users. Create a flexible, scalable storage aside powerful query for analyzing.
* 💡*Reading .png file in 📁assets for visualizing project.*
## Contributor
**luongngochuy1903**
