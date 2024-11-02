# Air-Quality-Improvement-Data-Analysis

This repository contains a data analysis project focused on examining air quality data from various geographical regions. By analyzing this data, we aim to identify areas of improvement in air quality, track air quality trends, and cluster regions with similar air quality patterns.

## Project Overview
The primary goal of this project is to analyze air quality data from sensor readings across different regions, calculate Air Quality Index (AQI), and identify trends in air quality improvements. Using clustering, we categorize geographical regions based on air quality data, and visually represent findings through histograms and geographical mappings.

## Features
1. **Data Acquisition**: Fetches 24-hour air quality data from [Dataset](https://data.sensor.community/static/v2/data.24h.json).
2. **Data Cleaning and Transformation**: Prepares data using PySpark, ensuring correct formats and handling of missing values.
3. **Air Quality Index (AQI) Calculation**: Computes AQI based on sensor data and classifies regions accordingly.
4. **Trend Analysis**: Calculates daily AQI and compares trends to highlight improvements in air quality.
5. **K-Means Clustering**: Groups regions into clusters based on geographical coordinates.
6. **Visualizations**:
   - Histogram of longest streaks of good air quality.
   - Geographical map displaying air quality data points.
7. **Top Countries and Regions**: Lists top 10 countries and top 50 regions with the best air quality.

## Technologies Used
- **Python**: Core programming language for data processing and analysis.
- **PySpark**: For large-scale data processing and analysis.
- **Geopandas**: For handling geographical data.
- **Matplotlib & Seaborn**: For visualizations.
- **Requests**: For fetching data from APIs.
- **KMeans Clustering**: For geographical clustering of regions.

## Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/tashi-2004/Air-Quality-Improvement-Data-Analysis
   cd Air-Quality-Improvement-Data-Analysis
