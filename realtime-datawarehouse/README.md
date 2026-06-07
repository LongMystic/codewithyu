# This project follows tutorial in Youtube: https://www.youtube.com/watch?v=fsH7wdHA1e8

## 1. Objective
Building real-time data warehouse and visualize it with near real-time dashboard 

## 2. Data
Generate data with python random library

## 3. Tech Stack
- Docker/Docker Compose: Deployment
- Apache Kafka: Event streaming / Message broker layer 
- Apache Pinot: real-time OLAP analytics database
  - Zookeeper: coordinates Pinot’s internal cluster state
- Apache Airflow: orchestration + scheduler
- Apache Superset: dashboard/ad-hoc analytic platform