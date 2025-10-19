# Analiza Prevara u Kreditnim Karticama# CSV -> MongoDB collections splitter



Repozitorijum sadrži MongoDB upite i analize za detekciju obrazaca prevara u transakcijama kreditnih kartica.This repository contains a script to split `credit_card_transactions_with_provider.csv` into five MongoDB collection JSON files:



## Struktura Projekta- `users.json`

- `locations.json`

### 📊 Analize po Query-jima- `credit_cards.json`

- `transactions.json`

- **Query 1** - Analiza prevara po brendovima kartica i kategorijama trgovaca- `merchants.json`

- **Query 2** - Geografska analiza hotspotova prevara po starosnim grupama  

- **Query 3** - Analiza udaljenosti između korisnika i trgovacaEach file is in MongoDB Extended JSON (one document per line) using typed values when possible: `$oid`, `$date`, `$numberLong`, `$numberDecimal`, `$numberInt`.

- **Query 4** - Profil TOP 10 žrtava prevara

Usage (PowerShell):

### 🛠️ Alati

```powershell

- `split_csv_pandas.py` - Skripta za podelu CSV fajla u normalizovane tabele za MongoDBpython .\create_collections.py --input credit_card_transactions_with_provider.csv --outdir output --maxrows 0

```

## Korišćeni Dataset

This will write the JSON files to `output\`.

Dataset sadrži 1.3M+ transakcija kreditnih kartica sa 0.58% fraud rate.

To import into MongoDB (example):

**Napomena:** Veliki CSV fajlovi su isključeni iz repozitorijuma zbog veličine.

```powershell

## Tehnologijemongoimport --db ccdb --collection users --file output\users.json --jsonArray

# or when using one-document-per-line extended json

- MongoDB (Aggregation Pipeline)mongoimport --db ccdb --collection users --file output\users.json --type json --jsonArray --legacy

- Python (pandas, CSV processing)```

- Studio 3T (za import i analizu)

Note: `mongoimport` options may require adjusting depending on your MongoDB version. The script uses deterministic hashed values for `_id` to keep relationships stable across runs but these are not real ObjectIds from MongoDB's driver.

---

*Kreirao: Miloš - Oktobar 2025*