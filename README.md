# Airflow Pipelines Project ![Apache Airflow](https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png)

This project demonstrates the use of **Apache Airflow** for automating and orchestrating data workflows. The repository contains several **DAGs (Directed Acyclic Graphs)** that execute various data pipelines, including interactions with Google Cloud services like **BigQuery** and **Cloud Data Fusion**.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies](#technologies)
- [Prerequisites](#prerequisites)

## Project Overview

The goal of this project is to create, manage, and execute **data pipelines** using **Apache Airflow**. We have built a collection of pipelines that are executed in Airflow, with tasks ranging from running SQL queries in BigQuery to launching pipelines in Cloud Data Fusion.

The DAGs in this repository are scheduled to run at different intervals, and they interact with various Google Cloud services to perform the following operations:
- Extracting data from **BigQuery**
- Launching **Cloud Data Fusion** pipelines
- Running custom Python operators for data manipulation

## Technologies

The project uses the following technologies:
- **Apache Airflow**: Orchestrates workflows, schedules DAGs, and handles task dependencies.
- **Google Cloud**: Services such as **BigQuery** and **Cloud Data Fusion** are integrated into the DAGs.
- **Docker**: Airflow is run locally using Docker and `docker-compose`.
- **Python**: Used for custom operators, transformations, and Airflow configuration.

## Prerequisites

Make sure you have the following installed on your local machine:
- **Docker** and **Docker Compose** (to run Airflow in containers)
- **Python 3.7+**
- **Google Cloud SDK** (if you are interacting with Google Cloud services)
- A **Google Cloud project** with proper permissions for **BigQuery** and **Cloud Data Fusion** (if required)
- An **Apache Airflow environment** (local setup or Google Cloud Composer)
