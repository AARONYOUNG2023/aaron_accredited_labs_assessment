
import os
import pandas as pd
import glob

# 1. Load central references
central_dir = "./processed_data/centralized"

df_customers = pd.read_csv \
    (os.path.join(central_dir, "customer_master.csv"))   # columns: [customer_id, customer_name, location, email]
df_pricing = pd.read_csv(os.path.join(central_dir, "equipment_pricing.csv"))  # columns: [branch, equipment_type, standard_price, accredited_price]
df_tech = pd.read_excel \
    (os.path.join(central_dir, "technician_training_matrix.xlsx"))  # columns: [technician, equipment_type]

# Create a set of (technician, equipment_type) pairs for easy checking
certified_pairs = set(zip(df_tech["technician"], df_tech["equipment_type"]))

# 2. Prepare output containers
invoice_rows = []
failed_rows = []

# 3. Loop over city folders
cities = ["Chicago", "Houston", "Los_Angeles", "New_York", "Phoenix"]
for city in cities:
    city_dir = os.path.join("./processed_data", city)

    # paths
    workorders_path = os.path.join(city_dir, "workorders", "workorders.csv")
    testpoints_path = os.path.join(city_dir, "test_points", "merged_test_points.csv")

    # skip if missing
    if not (os.path.exists(workorders_path) and os.path.exists(testpoints_path)):
        print(f"Skipping {city} - missing files.")
        continue

    df_work = pd.read_csv \
        (workorders_path)   # columns: [workorder_id, equipment_id, equipment_type, branch, customer_name, accredited, technician]
    df_test = pd.read_csv(testpoints_path)   # columns: [equipment_id, completed_date, testpoint_count]

    # Merge so each row from workorders has completed_date + testpoint_count
    merged = pd.merge(df_work, df_test, on="equipment_id", how="left")

    # 4. QC checks on each row
    for idx, row in merged.iterrows():
        fail_reasons = []

        workorder_id = row["workorder_id"]
        equipment_id = row["equipment_id"]
        equipment_type = row["equipment_type"]
        branch = row["branch"]
        customer_name = row["customer_name"]
        accredited_flag = str(row["accredited"]).lower()  # "yes" or "no" or something else
        technician = row["technician"]

        completed_date = row["completed_date"]
        testpoint_count = row["testpoint_count"]

        # -- (1) Check customer matches
        # For simplicity, use exact match on "customer_name"
        if not (df_customers["customer_name"] == customer_name).any():
            fail_reasons.append("customer_not_found")

        # -- (2) Technician certification
        # must appear in (technician, equipment_type) pairs
        if (technician, equipment_type) not in certified_pairs:
            fail_reasons.append("tech_not_certified")

        # -- (3) Completed date present
        if pd.isna(completed_date) or str(completed_date).strip() == "":
            fail_reasons.append("missing_completed_date")

        # -- (4) Pricing found
        # We need to find a row in df_pricing with same branch & equip_type
        price_subset = df_pricing[
            (df_pricing["branch"] == branch) &
            (df_pricing["equipment_type"] == equipment_type)
            ]
        price = None
        if len(price_subset) == 0:
            fail_reasons.append("pricing_not_found")
        else:
            # If we found exactly 1 row, pick the right column
            # If multiple rows or 1 row is found, we'll handle that carefully
            pr = price_subset.iloc[0]
            if accredited_flag == "yes":
                price = pr["accredited_price"]
            else:
                price = pr["standard_price"]

        # -- (5) Testpoint count requirement
        # Suppose "yes" => needs 15 points, "no" => needs 10
        # Adjust if your rules differ
        if accredited_flag == "yes":
            required_points = 15
        else:
            required_points = 10

        # if testpoint_count is not numeric or < required_points => fail
        if pd.isna(testpoint_count) or (testpoint_count < required_points):
            fail_reasons.append("insufficient_test_points")

        # If we have any failure, push to failed
        if fail_reasons:
            failed_rows.append({
                "workorder_id": workorder_id,
                "equipment_id": equipment_id,
                "equipment_type": equipment_type,
                "branch": branch,
                "customer_name": customer_name,
                "accredited": accredited_flag,
                "technician": technician,
                "failed_reasons": ";".join(fail_reasons)
            })
        else:
            # Otherwise, pass QC
            invoice_rows.append({
                "workorder_id": workorder_id,
                "equipment_id": equipment_id,
                "equipment_type": equipment_type,
                "branch": branch,
                "customer_name": customer_name,
                "accredited": accredited_flag,
                "technician": technician,
                "completed_date": completed_date,
                "testpoint_count": testpoint_count,
                "price": price
            })

# 5. After loop, build final DataFrames
df_invoices = pd.DataFrame(invoice_rows)
df_failed = pd.DataFrame(failed_rows)

# 6. Write them out
final_dir = "./processed_data/final"
os.makedirs(final_dir, exist_ok=True)
df_invoices.to_csv(os.path.join(final_dir, "invoices.csv"), index=False)
df_failed.to_csv(os.path.join(final_dir, "failed_qc.csv"), index=False)

print("Done! Created invoices.csv and failed_qc.csv with the new 5-step QC.")
