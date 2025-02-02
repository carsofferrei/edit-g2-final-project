# edit-g2-final-project

This project aim to have all the necessary code for the final project
[applied_project.pdf](https://github.com/user-attachments/files/18254966/applied_project.pdf)

# Usecase
Carris provides an API that delivers real-time data. The objectives are as follows:
- Streaming: Develop a real-time reporting system that calculates the average velocity, distance, time, and the next stop for buses. This step should produce a .py file and a notebook documenting the process to achieve these results.

- Airflow DAGs: Design and implement a batch dataflow pipeline. This will update information periodically, enabling the creation of fact and dimension tables to store historical measurements.

- DBT Project: Build a data transformation pipeline. Once the sources in the database are updated, DBT will be used to create models based on the information provided by the batch dataflow.

  Model Schema:
  ![er_schema](https://github.com/user-attachments/assets/62db63b1-d41c-4e38-a314-c1d6c155a6d0)


# All the data is stored on a GCP project.

# Tools:
- Google Cloud Platform
- Google Cloud Storage (GCS)
- BigQuery
- dbt Core
- Apache Airflow
- Python
- PySpark
- Google Colab
- Visual Studio Code
- GitHub
- Docker
