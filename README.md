# 🌀 Change Data Capture (CDC) Simulation using Databricks and PySpark

## 🎯 Objective

This project showcases the implementation of **Change Data Capture (CDC)** using **Delta Lake** and **Structured Streaming** on **Databricks**. It uses **mock UPI transaction data** to simulate real-time changes such as inserts, updates, and refunds, and demonstrates how to:

- Enable and consume the **Delta Change Data Feed (CDF)**
- Maintain an **aggregated merchant sales table** with net sales
- Implement incremental updates using **merge logic**

---

## 🗘️ Overview

This CDC simulation is built around two key components:

1. **Source Table (**``**)**:

   - Contains mock UPI transactions
   - CDC (Change Data Feed) is enabled
   - Supports updates for `transaction_status` (e.g., from `initiated` to `completed`, or `refunded`)

2. **Aggregated Table (**``**)**:

   - Maintains real-time aggregated metrics per merchant
   - Updates using `foreachBatch` and `merge` logic:
     - `total_sales`
     - `total_refunds`
     - `net_sales`

---

## ⚙️ Tech Stack

| Component         | Purpose                                  |
| ----------------- | ---------------------------------------- |
| **Databricks**    | Compute and orchestration                |
| **Delta Lake**    | Storage format with CDC enabled          |
| **PySpark**       | Data processing and streaming            |
| **Unity Catalog** | Table and schema management              |
| **Mock Data**     | Simulates real transaction change events |

---

## 📓 Notebooks Used

### 1. `upi_txn_data_mock.ipynb` (Mock Data Generator)

- Creates the source table `raw_upi_txns_v1` with CDC enabled.
- Inserts mock transaction batches simulating:
  - New transactions
  - Status updates
  - Refund events
- Uses Delta Lake’s **merge** to simulate transactional updates.

### 2. `cdc_merchant_aggregation.ipynb` (CDC Aggregation Pipeline)

- Creates the aggregated table `merchant_table`
- Reads changes from `raw_upi_txns_v1` using **Structured Streaming** with CDC:
  ```python
  spark.readStream.format('delta').option('readChangeFeed', 'true').table(source_table)
  ```
- Performs real-time aggregation in `foreachBatch`
- Uses Delta’s **merge** API to upsert metrics per merchant

---

## 🧪 CDC Flow Simulation

1. **Batch Inserts:** Add new transactions (`initiated` status)
2. **Batch Updates:** Change statuses to `completed`, `failed`, or `refunded`
3. **Stream Job:** Reads only changes (`insert`, `update_postimage`) and updates merchant-level aggregates
4. **Result:** Real-time updates to `merchant_table` showing live `total_sales`, `total_refunds`, and `net_sales`

---

## 📌 Key Features

- ✅ Delta Lake **Change Data Feed (CDF)** support
- ✅ **Idempotent upserts** via Delta's `merge` logic
- ✅ **Streaming + Batch** hybrid processing pattern
- ✅ Easily extensible for real-time dashboards, alerts, or ML integration

---

## ✅ Setup Instructions

1. Clone or open the notebooks in your Databricks workspace.
2. Run `upi_txn_data_mock.ipynb` to create the source table and insert mock data.
3. Run `cdc_merchant_aggregation.ipynb` to start the CDC stream and aggregate updates.
4. Observe live changes in the `merchant_table`.

---

## 📈 Output Example (Aggregated Table)

| merchant\_id | total\_sales | total\_refunds | net\_sales |
| ------------ | ------------ | -------------- | ---------- |
| M001         | 1000.00      | -500.00        | 500.00     |
| M003         | 1500.00      | 0.00           | 1500.00    |

---

## 📌 Conclusion

This project demonstrates how **Delta Lake's Change Data Feed (CDF)** can be used with PySpark to build reliable, real-time **aggregation pipelines**. Using **mock data**, we simulate UPI payment flows and refund scenarios to validate the CDC mechanism in Databricks.

