# Accredited Labs Automated Pipeline

This repository automates our calibration data flow, from raw inputs to final invoices:

1. **Raw Data**  
   - All new calibration requests (workorders, test_points, etc.) are uploaded into the `raw_data` folder, organized by city.  
   - The pipeline does _not_ modify files in `raw_data`; it simply reads them.

2. **Test & Development**  
   - Notebooks in the `test_code` folder are used to verify each step. We test cell by cell (fuzzy matching, combining test_points, validating QC) before adding them to the main pipeline.

3. **Processing & Pipeline**  
   - The final code lives in `pipeline_code/pipeline.ipynb`. Running it:
     1. Reads `raw_data`.
     2. Cleans/fuzzy‑matches `customer_name` to handle typos.
     3. Merges all `*_test_points.xlsx` per city into a single CSV, making it easier to track testpoint counts.
     4. Checks which work orders pass QC (go to `invoices.csv`) and which fail (go to `failed_qc.csv`).
     5. Creates/updates the database (`calibration_data.db`) in the `db` folder.

4. **Outputs**  
   - Cleaned files appear in `processed_data`.  
   - The pipeline also generates:
     - `invoices.csv` (1,532 records),
     - `failed_qc.csv` (452 records).
   - Both reflect final QC decisions.

## How to Use
- **Step 1:** Place raw files in `raw_data/{city}`.  
- **Step 2:** Open `pipeline_code/pipeline.ipynb` and run all cells.  
- **Step 3:** Check `db/calibration_data.db`, plus `final/invoices.csv` and `final/failed_qc.csv` for results.

This setup ensures **one-click** generation of the database and final CSV outputs, letting other departments upload data without worrying about the behind-the-scenes transformations.
