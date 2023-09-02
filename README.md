# Divvy ETL Project

Welcome to the Divvy End-to-End data engineering project! This project aims to automate the process of downloading, transforming, and analyzing Divvy bike data using a combination of technologies including Docker, Google Cloud, Prefect, and more.

## Project Overview
The project structure is organized as follows:

- **flows**: This directory contains the core workflow files and Docker deployment configurations.
  - **Dockerfile**: This file is used to build a Docker image for the parameterized data flow defined in the project.
  - **etl_parent_flow-deployment.yaml**: Metadata file providing deployment information for the Prefect flow.
  - **extract_load_transform.py**: This script automates the process of downloading Divvy monthly data and preparing it for transformation.
  - **etl_gcs_to_bg.py**: This script pulls data from Google Cloud Storage, transforms it, and loads it into BigQuery for analysis.
  - **D_param.py**: Parameterized ETL process file.
- **data**: This directory holds the downloaded Divvy bike data.

- **divvy_dbt**: This folder (still in development) will contain the transformation logic using DBT.

- **gcs**: Contains data downloaded from Google Cloud Storage for further transformation and loading into BigQuery.

- **docker-requirements.txt**: Requirements file specific to the Docker image creation process to optimize image download weight.

- **requirements.txt**: General project requirements for package dependencies.

- **Divvy_data.pdf**: Documentation detailing the Divvy bike data and its structure.

- **Divvy_Bike_Report.pdf**: Visualization and analysis report showcasing the insights derived from the transformed data.
## Project Goals and Technologies

The Divvy ETL project incorporates the following technologies:

- **Google Cloud**: Utilized as the project's data lake for storage and retrieval.
- **Google BigQuery**: Serves as the data warehouse for storing and querying transformed data.
- **DBT (Data Build Tool)**: To be used for data transformation, providing a structured and organized transformation layer.
- **Prefect**: Used for workflow orchestration and automation of the ETL pipeline.
- **Docker**: The parameterized data flow is encapsulated in a Docker container for simplified deployment and reproducibility.
- **Python**: The programming language driving the entire ETL process.

## Getting Started

To get started with the Divvy ETL project:

1. Make sure you have Docker, Google Cloud credentials, and Prefect installed.

2. Set up the required configurations in `etl_parent_flow-deployment.yaml`.

3. Adjust parameters in `D_param.py` to configure the ETL process according to your needs.

4. Run the Docker image creation process using the provided `Dockerfile`.

5. Execute the orchestrated Prefect flow to automate the ETL pipeline.

## Project Status

This project is currently under development. Updates and enhancements are being actively worked on.

Thank you for your interest in the Divvy ETL project! Your patience is appreciated as we work towards an exciting solution that integrates various technologies to provide valuable insights from Divvy bike data.

For more details, refer to the provided documentation files: `Divvy_data.pdf` for data specifics and `Divvy_Bike_Report.pdf` for visualizations and analysis.

**Note:** This README provides a high-level overview of the project structure and goals. For detailed instructions and explanations, refer to the project's internal files and documentation.
