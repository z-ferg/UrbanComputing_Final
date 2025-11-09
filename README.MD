# Analysis of Property Value on Access to Public Transportation in New York City

## Overview

This project investigates the connections between residential property values in New York City (NYC) and the property's access to public transportation (bus and subway lines). Utilizing spatial and property data from NYC Open Data, we aim to quantify the relationship between these two attributes and explore potential disparities in transit access relative to property values and community "status."

The core of the project involves calculating the shortest geographic distance from individual properties to the nearest bus stop and subway station and then using this data to build a regression model.

## Goals and Objectives

- **Quantify Access:** Compute the shortest distance from 9.8 million NYC properties to the nearest bus stop and subway entrance.
- **Establish Correlation:** Determine the correlation and relationship between a property's value and its proximity to public transit lines using a regression model.
- **Identify Disparity:** Analyze the results to determine if there is a disparity in access to public transit relative to the value of a property or group of properties, suggesting a relationship between a community's "status" and its public transit access.

## Distinction from Prior Work:

While related to existing research, notably "Public Transportation Accessibility, Proximity to New York City, and Residential Property Values in New Jersey" (Lee and Smart), our project is distinct in its scope and focus

| Feature | Our Project | Prior Work (Lee & Smart) |
| :--- | :--- | :--- |
| **Geography** | New York City properties and NYC Transit. | New Jersey properties and NJ/NYC Transit. |
| **Primary Goal** | Determine transit proximity given property value to find **correlation and disparity** (community status). | Determine property value given transit proximity for an improved **forecasting model**. |

## Methodology:
1. **Data Acquisition:** Download and import the three primary NYC Open Data datasets.
2. **Geospatial Processing:**
    - For each of the approximately 9.8 million properties (latitude/longitude), compute the haversine distance to every bus stop and subway entrance.
    - Identify and record the minimum (shortest) distance to the nearest bus stop and subway entrance, respectively.
3. **Data Integration:** Merge the property value data with the computed shortest distance metrics.
4. **Model Building:** Construct a regression model (e.g., Multiple Linear Regression or Spatial Lag Model) using property value as the dependent variable and the distance metrics as key independent variables.
5. **Analysis:** Interpret the model coefficients and statistical significance to draw conclusions about the relationship and identify disparities.

## Datasets

| Dataset Name | Source Link | Key Data Fields Used |
| :--- | :--- | :--- |
| **Property Values and Coordinates** | https://data.cityofnewyork.us/City-Government/Property-Valuation-and-Assessment-Data/yjxr-fw8i/about_data | Property Value, Latitude, Longitude |
| **Subway Station Entrances** | https://www.kaggle.com/datasets/new-york-state/nys-nyc-transit-subway-entrance-and-exit-data?resource=download | Latitude, Longitude, Station Name |
| **Bus Stops** | https://data.cityofnewyork.us/Transportation/Bus-Stop-Shelters/qafz-7myz | Latitude, Longitude, Route Information |

## Requirements and Setup
### Prerequisites
- Python 3.xx
- stable internet connection for initial data download.
Required Libraries
- pandas: For data manipulation and processing
- numpy: For numerical operations, especially distance calculations
- scipy: Potentially for spatial indexing to optimize distance calculations on large datasets
- statsmodels or scikit-learn: For implementing the regression model
- geopy: For calculating Haversine distance
- matplotlib / seaborn: For data visualization.

### Installation
Clone the repository

- git clone https://github.com/z-ferg/UrbanComputing_Final
- cd UrbanComputing_Final

# Install required Python packages
- pip install -r requirements.txt

# Authors
- James Agresto
- Zachary Ferguson
- Gavin Barro