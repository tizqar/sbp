#!/usr/bin/env python3
"""
Ispravljeni CSV splitter koji koristi pandas za čitanje originalnog CSV-a
"""
import pandas as pd
import csv
import os
import argparse
import hashlib

def generate_id(s: str) -> str:
    """Generate deterministic ID from string"""
    return hashlib.md5(s.encode('utf-8')).hexdigest()[:12]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', required=True, help='Input CSV file')
    parser.add_argument('--outdir', '-o', default='csv_corrected_v2', help='Output directory')
    parser.add_argument('--maxrows', type=int, default=0, help='Process at most N rows (0 = all)')
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print("Loading original CSV with pandas...")
    
    # Učitaj CSV sa pandas
    try:
        if args.maxrows > 0:
            df = pd.read_csv(args.input, nrows=args.maxrows)
        else:
            df = pd.read_csv(args.input)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return
    
    print(f"Loaded {len(df):,} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Proveri is_fraud kolonu
    if 'is_fraud' in df.columns:
        fraud_counts = df['is_fraud'].value_counts()
        print(f"is_fraud distribution: {dict(fraud_counts)}")
    else:
        print("WARNING: is_fraud column not found!")
        return

    users = {}
    locations = {}
    cards = {}
    merchants = {}
    transactions = []

    print("Processing transactions...")
    
    for idx, row in df.iterrows():
        if idx % 10000 == 0 and idx > 0:
            print(f"Processed {idx:,} rows")
        
        try:
            # Proveri da li su potrebni podaci dostupni
            if pd.isna(row.get('first')) or pd.isna(row.get('merchant')) or pd.isna(row.get('amt')):
                continue
            
            # Create location
            street = str(row.get('street', '')).strip()
            city = str(row.get('city', '')).strip()
            state = str(row.get('state', '')).strip()
            zip_code = str(row.get('zip', '')).strip()
            
            loc_key = f"{street}|{city}|{state}|{zip_code}"
            if loc_key not in locations:
                locations[loc_key] = {
                    '_id': generate_id(loc_key),
                    'street': street,
                    'city': city, 
                    'state': state,
                    'zip': zip_code,
                    'lat': str(row.get('lat', '')),
                    'long': str(row.get('long', '')),
                    'city_pop': str(row.get('city_pop', ''))
                }

            # Create user
            first = str(row.get('first', '')).strip()
            last = str(row.get('last', '')).strip()
            dob = str(row.get('dob', '')).strip()
            
            user_key = f"{first}|{last}|{dob}"
            if user_key not in users:
                users[user_key] = {
                    '_id': generate_id(user_key),
                    'first': first,
                    'last': last,
                    'gender': str(row.get('gender', '')).strip(),
                    'job': str(row.get('job', '')).strip(),
                    'dob': dob,
                    'location_id': locations[loc_key]['_id']
                }

            # Create credit card
            card_provider = str(row.get('card_provider', '')).strip()
            card_key = f"{user_key}|{card_provider}"
            if card_key not in cards:
                cards[card_key] = {
                    '_id': generate_id(card_key),
                    'cc_num': str(row.get('cc_num', '')).strip(),
                    'card_provider': card_provider,
                    'user_id': users[user_key]['_id']
                }

            # Create merchant
            merchant = str(row.get('merchant', '')).strip()
            merch_key = merchant
            if merch_key not in merchants:
                merchants[merch_key] = {
                    '_id': generate_id(merch_key),
                    'merchant': merchant,
                    'merch_lat': str(row.get('merch_lat', '')),
                    'merch_long': str(row.get('merch_long', ''))
                }

            # Create transaction - VAŽNO: koristiti originalne is_fraud vrednosti!
            is_fraud_value = str(row.get('is_fraud', '0')).strip()
            
            transactions.append({
                '_id': generate_id(f"{idx}|{row.get('trans_num', '')}"),
                'trans_date_trans_time': str(row.get('trans_date_trans_time', '')).strip(),
                'unix_time': str(row.get('unix_time', '')).strip(),
                'trans_num': str(row.get('trans_num', '')).strip(),
                'amt': str(row.get('amt', '')).strip(),
                'category': str(row.get('category', '')).strip(),
                'is_fraud': is_fraud_value,
                'transaction_channel': 'pos',
                'credit_card_id': cards[card_key]['_id'],
                'merchant_id': merchants[merch_key]['_id']
            })

        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            continue

    print(f"\nWriting CSV files...")
    
    # Write CSV files
    def write_csv(filename, data, fieldnames):
        filepath = os.path.join(args.outdir, filename)
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            if isinstance(data, dict):
                writer.writerows(data.values())
            else:
                writer.writerows(data)

    write_csv('users.csv', users, ['_id', 'first', 'last', 'gender', 'job', 'dob', 'location_id'])
    write_csv('locations.csv', locations, ['_id', 'street', 'city', 'state', 'zip', 'lat', 'long', 'city_pop'])
    write_csv('credit_cards.csv', cards, ['_id', 'cc_num', 'card_provider', 'user_id'])
    write_csv('merchants.csv', merchants, ['_id', 'merchant', 'merch_lat', 'merch_long'])
    write_csv('transactions.csv', transactions, ['_id', 'trans_date_trans_time', 'unix_time', 'trans_num', 'amt', 'category', 'is_fraud', 'transaction_channel', 'credit_card_id', 'merchant_id'])

    print(f"\nSUCCESS! Wrote CSV files to {args.outdir}:")
    print(f"  users.csv: {len(users):,} records")
    print(f"  locations.csv: {len(locations):,} records") 
    print(f"  credit_cards.csv: {len(cards):,} records")
    print(f"  merchants.csv: {len(merchants):,} records")
    print(f"  transactions.csv: {len(transactions):,} records")
    
    # Proveri fraud distribuciju u finalnim transakcijama
    fraud_dist = {}
    for trans in transactions:
        fraud_val = trans['is_fraud']
        fraud_dist[fraud_val] = fraud_dist.get(fraud_val, 0) + 1
    
    print(f"\nFraud distribution in final transactions:")
    for key, count in fraud_dist.items():
        perc = (count / len(transactions)) * 100
        print(f"  {key}: {count:,} ({perc:.2f}%)")
    
    print(f"\nFiles are ready for Studio 3T import!")

if __name__ == '__main__':
    main()