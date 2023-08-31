# Divvy ETL Project

Welcome to the Divvy ETL (Extract, Transform, Load) project! This project aims to automate the process of downloading, transforming, and analyzing Divvy bike data using a combination of technologies including Docker, Google Cloud, Prefect, and more.

## Project Overview
The project structure is organized as follows:

- **flows**: This directory contains the core workflow files and Docker deployment configurations.
  - **Dockerfile**: This file is used to build a Docker image for the parameterized data flow defined in the project.
  - **etl_parent_flow-deployment.yaml**: Metadata file providing deployment information for the Prefect flow.
  - **extract_load_transform.py**: This script automates the process of downloading Divvy monthly data and preparing it for transformation.
  - **etl_gcs_to_bg.py**: This script pulls data from Google Cloud Storage, transforms it, and loads it into BigQuery for analysis.
  - **D_param.py**: Parameterized ETL process file.
