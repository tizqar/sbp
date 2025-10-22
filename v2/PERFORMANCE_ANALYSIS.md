# Analiza Performansi: v1 vs v2 Optimizacija

## Poređenje Performansi

### Vreme Izvršavanja Upita

| Query | Opis | v1 Vreme | v2 Vreme | Ubrzanje | Poboljšanje |
|-------|------|----------|----------|----------|-------------|
| Query 1 | Prevare po Brendu Kartice | 59s | 1.2s | 49x | 98.0% |
| Query 2 | Top 10 Gradova po Prevarama | 157s | 1.0s | 157x | 99.4% |
| Query 3 | Demografska Analiza | 122s | 9ms | 13,556x | 99.99% |
| Query 4 | Profil Žrtava (Posao + Kategorija) | 123s | 77ms | 1,597x | 99.94% |
| Query 5 | Temporalni Obrasci (Noćna Analiza) | 385s | 49ms | 7,857x | 99.99% |

### Vizuelno Poređenje Performansi

```
Poređenje Vremena Izvršavanja (logaritamska skala)

Query 1:  ████████████████████████████████████████████████████ 59s
          █ 1.2s

Query 2:  ████████████████████████████████████████████████████████████████████████████████ 157s
          █ 1.0s

Query 3:  █████████████████████████████████████████████████████████████ 122s
          ▏ 9ms

Query 4:  █████████████████████████████████████████████████████████████ 123s
          ▏ 77ms

Query 5:  ████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████ 385s
          ▏ 49ms

Legenda: v1 (normalizovana šema) vs v2 (optimizovana šema)
```

## Metodologija Optimizacije

### Faza 1: Denormalizacija Šeme (transactions_enriched)

Kolekcija `transactions_enriched` je kreirana korišćenjem MongoDB Design Patterns:

**1. Extended Reference Pattern**
- Embedovana često korišćena polja iz povezanih kolekcija
- Denormalizovana polja: `card_provider`, `user_city`, `user_gender`, `user_job`, `user_id`
- Eliminisana potreba za $lookup operacijama

**2. Computed Pattern**
- Pre-kalkulisana polja u vreme upisa da bi se izbegla runtime kalkulacija
- Pre-computed polja: `user_age`, `user_age_group`, `amount_num`, `is_fraud_num`, `hour`, `transaction_year`
- Eliminisane skupe $addFields i $toInt/$toDouble operacije

**3. Attribute Pattern**
- Dodati specijalizovani atributi za specifične query paterne
- Fraud-specifična polja: `is_night`, `time_period`
- Omogućeno direktno filtriranje bez runtime parsiranja timestamp-a

### Faza 2: Strategija Indeksiranja

Kreirano 6 targetiranih indeksa na `transactions_enriched`:

```javascript
// Osnovni fraud filter
{ is_fraud_num: 1 }

// Query 1: Fraud po provideru
{ is_fraud_num: 1, card_provider: 1 }

// Query 2: Fraud po gradu
{ is_fraud_num: 1, user_city: 1 }

// Query 3: Demografija (compound 4-field index)
{ is_fraud_num: 1, user_age_group: 1, category: 1, user_gender: 1 }

// Query 4: Posao + Kategorija
{ is_fraud_num: 1, user_job: 1, category: 1 }

// Query 5: User + Noćni obrasci
{ is_fraud_num: 1, user_id: 1, is_night: 1 }
```

## Query-Specifična Analiza

### Query 1 & 2: Uticaj Denormalizacije

**Query 1 (Card Provider)** i **Query 2 (Gradovi)** su prvenstveno imali koristi od **denormalizacije šeme** umesto indeksiranja.

**v1 Uska grla:**
- Query 1: Zahtevao 1x $lookup (transactions → credit_cards)
- Query 2: Zahtevao 3x $lookup lanac (transactions → credit_cards → users → locations)
- $lookup operacije su skupe, posebno na 1.3M dokumenata

**v2 Poboljšanja:**
- Denormalizovano `card_provider` polje (Query 1)
- Denormalizovano `user_city` polje (Query 2)
- Eliminisane sve $lookup operacije
- Oba query-ja sada izvršavaju jednu $group operaciju na obogaćenim podacima

**Zašto Nema Korišćenja Indeksa:**
Oba query-ja analiziraju SVE transakcije (fraud + non-fraud) da bi izračunali tačne fraud procente:
```
fraud_percentage = (fraud_count / total_count) * 100
```
Ovo zahteva skeniranje cele kolekcije, ali denormalizacija je uklonila skupe JOIN-ove, što je rezultiralo u 49-157x ubrzanju.

### Query 3, 4, 5: Performanse Vođene Indeksima

**Query-ji 3, 4 i 5** su postigli dramatična ubrzanja (1,597x - 13,556x) kroz **kombinovanu denormalizaciju + indeksiranje**.

**Ključna Optimizacija:**
Sva tri query-ja prvo filtriraju samo fraud transakcije:
```javascript
{ $match: { is_fraud_num: 1 } }
```

**Uticaj:**
- Procesira samo 7,506 dokumenata (0.58% kolekcije) umesto 1,296,675
- Indeksi omogućavaju efikasno preuzimanje dokumenata
- Naredne $group operacije rade nad minimalnim setom podataka

**Query 3 Razrada:**
- Eliminisano: 2x $lookup, runtime kalkulacija starosti, age_group klasifikacija
- Index: index sa 4 polja
- Rezultat: 122s → 9ms (13,556x brže)

**Query 4 Razrada:**
- Eliminisano: 2x $lookup operacije
- Index: Compound index na posao + kategorija
- Rezultat: 123s → 77ms (1,597x brže)

**Query 5 Razrada:**
- Eliminisano: 2x $lookup, runtime ekstrakcija sata, is_night kalkulacija
- Index: User + night flag index 
- Rezultat: 385s → 49ms (7,857x brže)

## Kompromisi i Razmatranja

### Storage vs Performanse
- **Povećanje storage-a:** ~30% (denormalizovana polja)
- **Query performanse:** 49x do 13,556x poboljšanje
- **Verdict kompromisa:** Prihvatljivo - dobici u read performansama daleko prevazilaze troškove čuvanja podataka

### Konzistentnost Podataka
- Denormalizovani podaci zahtevaju sinhronizaciju update-a
- ETL proces mora da održava integritet podataka

