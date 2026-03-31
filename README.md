````markdown
# Hands-on L10: Spark Structured Streaming + Machine Learning with MLlib

## Overview
This project implements a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming and Spark MLlib[cite: 3]. It processes streaming data, implements machine learning algorithms, and predicts various features[cite: 3].

## Project Structure
* `data_generator.py`: A Python script that simulates real-time ride-sharing data[cite: 3].
* `task4.py`: Script for real-time fare prediction using a pre-trained MLlib regression model.
* `task5.py`: Script for time-based fare trend prediction using windowed feature engineering.
* `training-dataset.csv`: Static historical dataset used to train the machine learning models offline[cite: 3].
* `models/`: Directory where the trained MLlib models (`fare_model` and `fare_trend_model_v2`) are stored.

## Execution Instructions
To run the analytics pipeline, open your Codespace (or local environment) and execute the following commands in three separate terminal windows simultaneously:

1. **Terminal 1 (Data Stream):** 
   ```bash
   python data_generator.py
````

2.  **Terminal 2 (Task 4 Inference):**
    ```bash
    python task4.py
    ```
3.  **Terminal 3 (Task 5 Inference):**
    ```bash
    python task5.py
    ```

-----

## Task 4: Real-Time Fare Prediction Using MLlib Regression

**Approach:**

  * **Offline Model Training:** A LinearRegression model is trained using a static CSV file (`training-dataset.csv`)[cite: 3]. The model learns the relationship between `distance_km` (the feature) and `fare_amount` (the label)[cite: 3]. The trained model is then saved to disk[cite: 3].
  * **Real-Time Inference:** The script ingests live ride data from the socket[cite: 3]. For each incoming ride, it uses the pre-trained model to predict the fare based on the trip's distance[cite: 3]. It then calculates the deviation between the actual fare and the predicted fare to identify potential anomalies[cite: 3].

**Results:**
*!![Task 4 Output][alt text](<Screenshot 2026-03-31 161412.png>)*

-----

## Task 5: Time-Based Fare Trend Prediction

**Approach:**

  * **Offline Model Training:** The training data from `training-dataset.csv` is first aggregated into 5-minute windows, calculating the `avg_fare` for each[cite: 3]. Instead of using a raw timestamp, we perform feature engineering, creating cyclical features like `hour_of_day` and `minute_of_hour` from the window's start time[cite: 3]. A Linear Regression model is trained on these features and saved[cite: 3].
  * **Real-Time Inference:** The live stream is aggregated using the same 5-minute windowing logic[cite: 3]. The same `hour_of_day` and `minute_of_hour` features are created for each window[cite: 3]. The pre-trained model is then used to predict the `avg_fare` for that time window[cite: 3].

**Results:**
*!![Task 5 Output][alt text](<Screenshot 2026-03-31 161430.png>)*

```
```