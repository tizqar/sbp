# Query 1: Analiza Prevara po Brendu Kartice

## Šta ovaj upit radi?

Analizira sve kreditne kartice i prikazuje koji brend (Visa, Mastercard, AmEx, Discover) ima najviše prevara. Za svaki brend dobijamo ukupan broj transakcija, broj prevara, i procenat prevarantskih transakcija.

## Rezultati

- vreme izvršavanja 1s

## MongoDB Kod

```javascript
db.transactions_enriched.aggregate([
  {
    $group: {
      _id: "$card_provider",
      total_transactions: { $sum: 1 },
      fraud_transactions: { $sum: "$is_fraud_num" }
    }
  },
  {
    $project: {
      _id: 0,
      provider: "$_id",
      total_transactions: 1,
      fraud_transactions: 1,
      fraud_percentage: {
        $round: [
          {
            $multiply: [
              { $divide: ["$fraud_transactions", "$total_transactions"] }, 
              100
            ]
          },
          2
        ]
      }
    }
  },
  { $sort: { fraud_percentage: -1 } }
])
```

