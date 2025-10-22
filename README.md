# Analiza Prevara u Kreditnim Karticama# CSV -> MongoDB collections splitter



Repozitorijum sadrži MongoDB upite i analize za detekciju obrazaca prevara u transakcijama kreditnih kartica.Skripta za deljenje početnog csv na 5 kolekcija `credit_card_transactions_with_provider.csv` 

## Struktura Projekta
- `users.json`

- `locations.json`

- `credit_cards.json`

- `transactions.json`

- `merchants.json`
### 📊 Analize po Query-jima

- **Query 1** Za svaku vrstu kartice (Visa, MasterCard, AmEx, Discover), izračunaj procenat prevarantskih transakcija.

- **Query 2** - Identifikuj 10 gradova sa najvećim brojem prevarantskih transakcija.

- **Query 3** Koje starosne grupe imaju najveću učestalost prevara, grupisano po kategorijama trgovaca i polu korisnika?

- **Query 4**  Profil TOP 10 žrtava prevara
- **Query 5** identifikuje top 10 korisnika sa najviše prevarantskih transakcija, analizira njihov profil, lokaciju, finansijske gubitke, i temporalne obrasce (fokus na noćne prevare 18-24h)

Usage (PowerShell):

### 🛠️ Alati

```powershell

- `split_csv_pandas.py` - Skripta za podelu CSV fajla u normalizovane tabele za MongoDBpython .\create_collections.py --input credit_card_transactions_with_provider.csv --outdir output --maxrows 0

```

## Korišćeni Dataset


Dataset sadrži 1.3M+ transakcija kreditnih kartica sa 0.58% fraud rate.

To import into MongoDB (example):

**Napomena:** Veliki CSV fajlovi su isključeni iz repozitorijuma zbog veličine.

```powershell

## Tehnologijemongoimport --db ccdb --collection users --file output\users.json --jsonArray

# or when using one-document-per-line extended json

- MongoDB (Aggregation Pipeline)mongoimport --db ccdb --collection users --file output\users.json --type json --jsonArray --legacy

- Python (pandas, CSV processing)```

---

*Kreirao: Miloš - Oktobar 2025*