# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. 📁 Project Structure**

```
Spark_L9/
├── checkpoints/              # Spark checkpoint directories (auto-generated, excluded from git)
│   ├── task_1/
│   ├── task_2/
│   └── task_3/
├── outputs/                  # Output CSV files
│   ├── task_1/
│   │   ├── batch_0/
│   │   ├── batch_1/
│   │   └── batch_2/
│   ├── task_2/
│   │   ├── batch_0/
│   │   ├── batch_1/
│   │   └── batch_2/
│   └── task_3/
│       ├── batch_6/
│       ├── batch_7/
│       └── batch_8/
├── task1.py                  # Basic streaming ingestion
├── task2.py                  # Real-time aggregations
├── task3.py                  # Windowed time-based analytics
├── data_generator.py         # Streaming data generator
├── .gitignore               # Git ignore file
└── README.md                # This file
```

### Key Components:
- **data_generator.py**: Simulates real-time ride-sharing data and streams via socket (localhost:9999)
- **task1.py**: Ingests and parses streaming JSON data
- **task2.py**: Performs driver-level aggregations
- **task3.py**: Analyzes trends using time windows
- **checkpoints/**: Spark streaming checkpoints for fault tolerance (NOT committed to git)
- **outputs/**: Processed results in CSV format

---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**
**Goal**: Connect to streaming data source and parse JSON messages

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Instructions:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Print the parsed data to the console (using .writeStream.format("console")).

**Key Concepts**: Socket streaming, JSON parsing, schema definition
---

## **Task 2: Real-Time Aggregations (Driver-Level)**
**Goal**: Compute real-time statistics grouped by driver

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Instructions:**
1. Reuse the parsed DataFrame from Task 1.
2. Group by driver_id and compute:
3. SUM(fare_amount) as total_fare
4. AVG(distance_km) as avg_distance
5. Store the result in csv

**Key Concepts**: GroupBy aggregations, streaming state management
---

## **Task 3: Windowed Time-Based Analytics**
**Goal**: Analyze fare trends over time using sliding windows

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

**Key Concepts**: Time windows, watermarking, event-time processing
---
## 🎯 Running the Pipeline

### Important: Run in Separate Terminals!

Each component needs its own terminal window. Do NOT run multiple tasks simultaneously as they compete for the same socket connection.

### Terminal 1: Start Data Generator
```bash
python data_generator.py
```
**Expected Output**:
```
Streaming data to localhost:9999...
New client connected: ('127.0.0.1', 54321)
Sent: {'trip_id': 'abc-123', 'driver_id': 42, ...}
Sent: {'trip_id': 'def-456', 'driver_id': 17, ...}
```
**Keep this running throughout all tasks!**

### Terminal 2: Run Task 1 (2-3 minutes)
```bash
python task1.py
```

**Expected Output**:
```
Streaming started. Waiting for data from localhost:9999...
Press Ctrl+C to stop.

==================================================
Batch 0: 1 rows
==================================================
+------------------------------------+---------+-----------+-----------+-------------------+
|trip_id                             |driver_id|distance_km|fare_amount|timestamp          |
+------------------------------------+---------+-----------+-----------+-------------------+
|2e778335-fce8-4ef8-af6e-a48bb247904f|100      |28.15      |14.77      |2025-10-14 17:14:09|
+------------------------------------+---------+-----------+-----------+-------------------+
✓ Batch 0 saved to outputs/task_1/batch_0/

==================================================
Batch 1: 2 rows
==================================================
+------------------------------------+---------+-----------+-----------+-------------------+
|trip_id                             |driver_id|distance_km|fare_amount|timestamp          |
+------------------------------------+---------+-----------+-----------+-------------------+
|6cabc2c8-445b-4501-8f36-a28fbb9fe9f2|58       |14.56      |45.46      |2025-10-14 17:14:10|
|44cc9720-f651-41b8-ad8c-e515353aca4b|36       |13.4       |98.74      |2025-10-14 17:14:11|
+------------------------------------+---------+-----------+-----------+-------------------+
✓ Batch 1 saved to outputs/task_1/batch_1/
```

**Stop after 2-3 minutes** (Ctrl+C) once you have several batches.

### Terminal 3: Run Task 2 (2-3 minutes)
```bash
python task2.py
```

**Expected Output**:
```
Streaming started. Waiting for data from localhost:9999...
Press Ctrl+C to stop.

==================================================
Batch 0: 5 rows
==================================================
+---------+----------+------------------+
|driver_id|total_fare|avg_distance      |
+---------+----------+------------------+
|42       |145.30    |15.75             |
|17       |89.50     |12.30             |
|58       |234.80    |18.45             |
|100      |67.20     |9.15              |
|36       |198.74    |13.40             |
+---------+----------+------------------+
✓ Batch 0 saved to outputs/task_2/batch_0/

==================================================
Batch 1: 8 rows
==================================================
+---------+----------+------------------+
|driver_id|total_fare|avg_distance      |
+---------+----------+------------------+
|42       |289.60    |16.20             |
|17       |178.90    |11.85             |
...
✓ Batch 1 saved to outputs/task_2/batch_1/
```

**Stop after 2-3 minutes** once aggregations stabilize.

---

### Terminal 4: Run Task 3 (10+ minutes)
```bash
python task3.py
```

**Expected Output (First 5-6 minutes - EMPTY is normal!)**:
```
Streaming started. Waiting for data from localhost:9999...
Press Ctrl+C to stop.

-------------------------------------------
Batch: 0
-------------------------------------------
+------------+----------+----------+
|window_start|window_end|total_fare|
+------------+----------+----------+
+------------+----------+----------+
Batch 0 is empty, skipping write

[Batches 1-5 will also be empty - this is expected!]
```

**After 6+ minutes - RESULTS APPEAR**:
```
-------------------------------------------
Batch: 6
-------------------------------------------
+-------------------+-------------------+----------+
|window_start       |window_end         |total_fare|
+-------------------+-------------------+----------+
|2025-10-14 17:22:00|2025-10-14 17:27:00|1247.80   |
+-------------------+-------------------+----------+
✓ Batch 6 written to CSV with 1 rows
```

**⚠️ CRITICAL**: Run Task 3 for at least **10 minutes** to get meaningful results!

---

## 🛠️ Troubleshooting

### Common Issues and Solutions


#### Issue 1: "Multiple streaming queries using metadata"
**Problem**: Two queries trying to use same checkpoint
```bash
# Solution: Clean checkpoints
rm -rf checkpoints/task_X
mkdir -p checkpoints/task_X
# Run the task again
```

#### Issue 2: Empty CSV files (only headers)
**Problem**: Code only writing first batch 0.
```bash
# Solution: Use updated task files with foreachBatch
# Clean and restart
rm -rf checkpoints/task_1 outputs/task_1
mkdir -p checkpoints/task_1 outputs/task_1
python task1.py
```
---

## 📬 Submission Checklist

- [✅] Python scripts 
- [✅] Output files in the `outputs/` directory  
- [✅] Completed `README.md`  
- [✅] Commit everything to GitHub Classroom  
- [✅] Submit your GitHub repo link on canvas

---

# Spark_L9
