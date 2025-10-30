"""Generate 1M transaction dataset with fraud patterns."""
import csv
import random
from datetime import datetime, timedelta

# Configuration
NUM_TRANSACTIONS = 1_000_000
NUM_USERS = 10_000
OUTPUT_FILE = "transactions_1m.csv"

# Merchants by category
NORMAL_MERCHANTS = [
    "Starbucks", "Walmart", "Target", "Costco", "Whole Foods",
    "Shell Gas", "Chevron", "McDonald's", "Chipotle", "Safeway",
    "CVS Pharmacy", "Walgreens", "Home Depot", "Lowe's", "IKEA",
    "Nordstrom", "Macy's", "Gap", "H&M", "Trader Joe's"
]

FRAUD_MERCHANTS = [
    "Casino Royal", "Bitcoin Exchange", "Crypto.com", "Luxury Cars Inc",
    "Electronics Warehouse", "Best Buy", "Apple Store", "Jewelry Palace"
]

# Generate users
users = [f"user_{str(i).zfill(5)}" for i in range(NUM_USERS)]

# Start date
start_date = datetime(2025, 1, 1, 0, 0, 0)

print(f"Generating {NUM_TRANSACTIONS:,} transactions...")

with open(OUTPUT_FILE, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['user_id', 'timestamp', 'merchant_name', 'amount'])

    for i in range(NUM_TRANSACTIONS):
        if i % 100_000 == 0:
            print(f"  Progress: {i:,} / {NUM_TRANSACTIONS:,}")

        user_id = random.choice(users)

        # Random timestamp over ~300 days
        days_offset = random.uniform(0, 300)
        hours_offset = random.uniform(0, 24)
        timestamp = start_date + timedelta(days=days_offset, hours=hours_offset)

        # Inject fraud patterns (5% fraud rate)
        is_fraud = random.random() < 0.05

        if is_fraud:
            fraud_type = random.choice(['velocity', 'high_value', 'nighttime', 'merchant_rep'])

            if fraud_type == 'velocity':
                # Velocity spike: 5 transactions in 10 minutes
                base_time = timestamp
                for j in range(5):
                    merchant = random.choice(NORMAL_MERCHANTS)
                    amount = round(random.uniform(50, 300), 2)
                    txn_time = base_time + timedelta(minutes=j*2)
                    writer.writerow([user_id, txn_time.strftime('%Y-%m-%d %H:%M:%S'), merchant, amount])
                continue

            elif fraud_type == 'high_value':
                # High value anomaly
                merchant = random.choice(FRAUD_MERCHANTS)
                amount = round(random.uniform(2000, 10000), 2)

            elif fraud_type == 'nighttime':
                # Nighttime high value
                night_hour = random.randint(2, 4)
                timestamp = timestamp.replace(hour=night_hour)
                merchant = random.choice(FRAUD_MERCHANTS)
                amount = round(random.uniform(1000, 5000), 2)

            elif fraud_type == 'merchant_rep':
                # Merchant repetition: 7 transactions at same merchant in 1 hour
                merchant = random.choice(NORMAL_MERCHANTS)
                base_time = timestamp
                for j in range(7):
                    amount = round(random.uniform(5, 50), 2)
                    txn_time = base_time + timedelta(minutes=j*8)
                    writer.writerow([user_id, txn_time.strftime('%Y-%m-%d %H:%M:%S'), merchant, amount])
                continue
        else:
            # Normal transaction
            merchant = random.choice(NORMAL_MERCHANTS)

            # Normal amounts follow log-normal distribution
            amount = round(random.lognormvariate(3.5, 1.0), 2)
            amount = max(1.0, min(amount, 500.0))  # Cap at $500

        writer.writerow([user_id, timestamp.strftime('%Y-%m-%d %H:%M:%S'), merchant, amount])

print(f"\n✓ Generated {NUM_TRANSACTIONS:,} transactions")
print(f"✓ Saved to {OUTPUT_FILE}")
print(f"✓ File size: ~{NUM_TRANSACTIONS * 80 / 1024 / 1024:.1f} MB")
