import pandas as pd
import os
import json

CSV_DIR = 'data/raw/csv/'
JSON_DIR = 'data/raw/json/'

os.makedirs(JSON_DIR, exist_ok=True)

csv_files = [f for f in os.listdir(CSV_DIR) if f.endswith('.csv')]

for file in csv_files:
    input_path = os.path.join(CSV_DIR, file)
    output_path = os.path.join(JSON_DIR, file.replace('.csv', '.json'))

    try:
        df = pd.read_csv(input_path)
        df.to_json(output_path, orient='records', lines=True)
        print(f"✅ Converted {file} → {output_path}")
    except Exception as e:
        print(f"❌ Failed to convert {file}: {e}")

