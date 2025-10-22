# Query 2: Top 10 Gradova po Broju Prevara

## Šta ovaj upit radi?

Prikazuje 10 gradova u USA sa najvećim brojem prevarantskih transakcija. Za svaki grad vidimo ukupan broj transakcija iz tog grada i koliko od njih su bile prevare.

## Rezultati

![Query 2 Results](output.png)

- vreme izvršavanja 1.3s


## MongoDB Kod

```javascript
db.transactions_enriched.aggregate([
  {
    $group: {
      _id: "$user_city",
      total_transactions: { $sum: 1 },
      fraud_transactions: { $sum: "$is_fraud_num" }
    }
  },
  {
    $project: {
      _id: 0,
      city: "$_id",
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
  {
    $match: {
      city: { $ne: null, $ne: "" }
    }
  },
  { $sort: { fraud_transactions: -1 } },
  { $limit: 10 }
])
```

## Šta je novo u v2?

✅ Eliminisana 3 JOIN operacije (transactions → credit_cards → users → locations)  
✅ Grad korisnika već dostupan u svakoj transakciji  
⚡ **Vreme:** ~600ms (analizira svih 1.3M transakcija)
