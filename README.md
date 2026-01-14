\# Auto Loan ETL Pipeline

![etl](https://github.com/user-attachments/assets/69b8f9a0-283c-4b8f-9626-edb2e55efa29)

\## Project Description

This project implements an end-to-end ETL pipeline for auto loan data.

It extracts data from CSV files, transforms it, and loads analytics-ready datasets.




\## Project Structure
auto-loan-data-pipeline/
 ├─ data/              # Source CSV files
 
 ├─ dags/
 
 │   └─ daily\_auto\_loan\_etl.py    # Airflow DAG definition \& schedule
 
 ├─ etl/               # Python ETL scripts
 
 │   ├─ main.py
 
 │   ├─ extract.py
 
 │   ├─ transform.py
 
 │   ├─ load.py
 
 │   ├─ s3\_ingestion.py
 
 │   ├─ check\_s3.py
 
 │   └─ output/            # Generated fact and dimension CSVs
 
 ├─ .env
 
 └─ README.md



\## Technologies Used

\- Orchestration: Apache Airflow

\- Containerization: Docker \& Docker Compose

\- Programming: Python (Pandas)

\- Cloud Platforms: AWS (S3)

\- Library: Boto3 (AWS SDK for Python), Pandas, Pathlib

\- Environment: Docker

\- Source Format: CSV (Local File System)


\## How to Run

1\. Open terminal in project root folder

2\. Virtual Environment: Activate your virtual environment (e.g., venv or conda) if available

3\. Activate virtual environment if available

4\. Environment Setup: Create a .env file in the root directory and input your Cloud Credentials (AWS \& Azure)

5\. Install dependencies: pip install pandas boto3 python-dotenv azure-storage-file-datalake

6\. Archive to AWS : python etl/s3\_ingestion.py

7\. Run ETL: python etl/main.py

8\. Check generated CSVs in output/ folder

9\. Process to Azure : python etl/main.py

10\. Check Results : Verify generated CSVs in etl/data/ and check your Cloud Dashboards

<img width="532" height="347" alt="image" src="https://github.com/user-attachments/assets/69bc154d-60ef-4a2d-9e03-e82bf8fba468" />

<img width="1543" height="689" alt="image" src="https://github.com/user-attachments/assets/470e97b2-34c0-4445-86e5-cab779eefad8" />

<img width="578" height="353" alt="image" src="https://github.com/user-attachments/assets/685f0005-d5cc-4109-b40e-0600c9ae8d0c" />

<img width="1006" height="121" alt="image" src="https://github.com/user-attachments/assets/d383bcea-ad8b-4632-ba37-d56c4575e41f" />



