import boto3
import random
import time

timestream = boto3.client('timestream-write')

DATABASE_NAME = 'ecomdb_simple' 
TABLE_NAME = 'inventory_metrics_simple'  

def lambda_handler(event, context):
    current_time = str(int(time.time() * 1000))  

    records = []
    for _ in range(5):
        product_id = f"P{random.randint(1000, 9999)}"
        seller_id = f"S{random.randint(100, 999)}"
        price = round(random.uniform(20, 200), 2)
        stock_level = random.randint(1, 100)

        record = {
            'Dimensions': [  
                {'Name': 'product_id', 'Value': product_id},
                {'Name': 'seller_id', 'Value': seller_id}
            ],
            'MeasureName': 'inventory',
            'MeasureValueType': 'MULTI',
            'MeasureValues': [
                {'Name': 'price', 'Value': str(price), 'Type': 'DOUBLE'},
                {'Name': 'stock_level', 'Value': str(stock_level), 'Type': 'BIGINT'}
            ],
            'Time': current_time
        }

        records.append(record)

    try:
        response = timestream.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            Records=records
        )
        return {
            'statusCode': 200,
            'body': f"Inserted {len(records)} records."
        }

    except timestream.exceptions.RejectedRecordsException as e:
        # Log rejected records for debugging
        print(f"RejectedRecords: {e.response['RejectedRecords']}")
        return {
            'statusCode': 400,
            'body': f"Write failed: {e}"
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {e}"
        }
