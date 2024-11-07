# Air-Quality-Improvement-Data-Analysis

This repository contains a data analysis project focused on examining air quality data from various geographical regions. By analyzing this data, we aim to identify areas of improvement in air quality, track air quality trends, and cluster regions with similar air quality patterns.

## Project Overview
The primary goal of this project is to analyze air quality data from sensor readings across different regions, calculate Air Quality Index (AQI), and identify trends in air quality improvements. Using clustering, we categorize geographical regions based on air quality data, and visually represent findings through histograms and geographical mappings.

## Files Included
1. **Datasets:**
   -  Averaged Data from last 24 hours for each sensor: [Visit Here](https://data.sensor.community/static/v2/data.24h.json)
   -  Averaged Data from last 5 minutes for each sensor (for testing): [Visit Here](https://data.sensor.community/static/v2/data.json)

2. **Reports:**
   - `Report.pdf`: A comprehensive report detailing the analysis, visualizations, and insights.

3. **Code:**
   - `code.py`: This script performs the entire analysis, from data ingestion and preprocessing to visualization and reporting.
     
## Features
1. **Data Acquisition**: Fetches 24-hour air quality data and averaged data from the last 5 minutes for each sensor.
2. **Data Cleaning and Transformation**: Prepares data using PySpark, ensuring correct formats and handling of missing values.
3. **Air Quality Index (AQI) Calculation**: Computes AQI based on sensor data and classifies regions accordingly.
4. **Trend Analysis**: Calculates daily AQI and compares trends to highlight improvements in air quality.
5. **K-Means Clustering**: Groups regions into clusters based on geographical coordinates.
6. **Visualizations**:
   - Histogram of longest streaks of good air quality.
     <img width="975" alt="3" src="https://github.com/user-attachments/assets/68c9e02b-04d1-42fd-b661-c75b6c9be79d">
   - Geographical map displaying air quality data points.
     <img width="975" alt="4" src="https://github.com/user-attachments/assets/5be7e3bb-800c-405b-a84a-d2c1d1de6f82">
7. **Top Countries and Regions**: Lists top 10 countries and top 50 regions with the best air quality.
   - <img width="975" alt="1" src="https://github.com/user-attachments/assets/db640d5d-d70c-4668-a865-44eac9ae7083">
   - <img width="975" alt="2a" src="https://github.com/user-attachments/assets/dfca5ef1-7ef0-4920-9d85-fb5e0ace4229">
   - <img width="975" alt="2b" src="https://github.com/user-attachments/assets/0cd7d0d9-1ab6-4c8d-be6b-71fcc147c107">

## Technologies Used
- **Python**: Core programming language for data processing and analysis.
- **PySpark**: For large-scale data processing and analysis.
- **Geopandas**: For handling geographical data.
- **Matplotlib & Seaborn**: For visualizations.
- **Requests**: For fetching data from APIs.
- **KMeans Clustering**: For geographical clustering of regions.

## Contributors
- Tashfeen Abbasi
- [Laiba Mazhar](https://github.com/laiba-mazhar)
   
## Contact
For any questions or suggestions, feel free to contact at [abbasitashfeen7@gmail.com]
