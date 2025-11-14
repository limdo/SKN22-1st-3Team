import csv

data_list = []

with open("vehicle_registration_monthly_sum.csv", "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)

    for row in reader:

        # 빈 row 또는 key 없는 row 방지
        if not row or row["year_month"] == "" or row["year_month"] is None:
            continue

        data_list.append({
            "year_month": row["year_month"],
            "vehicle_type": row["vehicle_type"],
            "registration_cnt": int(row["registration_cnt"])
        })

print(data_list, type(data_list))