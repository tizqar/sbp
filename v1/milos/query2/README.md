# Geografska Analiza Prevara po Starosnim Grupama

Ovaj upit identifikuje geografske hotspotove prevara - gradove i države gde je procenat prevarantskih transakcija značajno iznad globalnog proseka. Posebno me zanima kako se ovo razlikuje po starosnim grupama korisnika.

## Cilj Analize

Tražim lokacije gde je fraud rate **minimum 2x veći** od globalnog proseka, grupisano po starosnim grupama (18-24, 25-34, itd.). Ovo mi može pomoći da identifikujem:

- Da li mlađi ili stariji korisnici su više ciljani u određenim regijama
- Koji gradovi/države predstavljaju najveći rizik
- Kakvi su prosečni iznosi prevara u tim hotspot lokacijama

## MongoDB Upit

```javascript
// Analiza geografskih hotspotova prevara po starosnim grupama
db.transactions.aggregate([
  // Spoji sa karticama i korisnicima
  {
    $lookup: {
      from: "credit_cards",
      localField: "credit_card_id", 
      foreignField: "_id",
      as: "card"
    }
  },
  {$unwind: "$card"},
  
  {
    $lookup: {
      from: "users",
      localField: "card.user_id", 
      foreignField: "_id",
      as: "user"
    }
  },
  {$unwind: "$user"},
  
  {
    $lookup: {
      from: "locations",
      localField: "user.location_id", 
      foreignField: "_id",
      as: "location"
    }
  },
  {$unwind: "$location"},
  
  // Kalkuliši starost korisnika
  {
    $addFields: {
      is_fraud_num: {$toInt: "$is_fraud"},
      amount_num: {$toDouble: "$amt"},
      birth_year: {
        $toInt: {
          $substr: ["$user.dob", 0, 4]
        }
      },
      transaction_year: {
        $toInt: {
          $substr: ["$trans_date_trans_time", 0, 4]
        }
      }
    }
  },
  
  // Dodaj starosnu grupu
  {
    $addFields: {
      user_age: {$subtract: ["$transaction_year", "$birth_year"]},
      age_group: {
        $switch: {
          branches: [
            { case: { $lt: ["$user_age", 25] }, then: "18-24" },
            { case: { $lt: ["$user_age", 35] }, then: "25-34" },
            { case: { $lt: ["$user_age", 45] }, then: "35-44" },
            { case: { $lt: ["$user_age", 55] }, then: "45-54" },
            { case: { $lt: ["$user_age", 65] }, then: "55-64" }
          ],
          default: "65+"
        }
      }
    }
  },
  
  // Prvo kalkuliši globalni prosek
  {
    $facet: {
      "global_stats": [
        {
          $group: {
            _id: null,
            total_transactions: {$sum: 1},
            total_fraud: {$sum: "$is_fraud_num"}
          }
        },
        {
          $addFields: {
            global_fraud_rate: {
              $divide: ["$total_fraud", "$total_transactions"]
            }
          }
        }
      ],
      "location_analysis": [
        // Grupiši po gradu/državi i starosnoj grupi
        {
          $group: {
            _id: {
              city: "$location.city",
              state: "$location.state", 
              age_group: "$age_group"
            },
            total_transactions: {$sum: 1},
            fraud_transactions: {$sum: "$is_fraud_num"},
            total_fraud_amount: {
              $sum: {
                $cond: [
                  {$eq: ["$is_fraud_num", 1]}, 
                  "$amount_num", 
                  0
                ]
              }
            }
          }
        },
        {
          $addFields: {
            fraud_rate: {
              $divide: ["$fraud_transactions", "$total_transactions"]
            },
            avg_fraud_amount: {
              $cond: [
                {$gt: ["$fraud_transactions", 0]},
                {$divide: ["$total_fraud_amount", "$fraud_transactions"]},
                0
              ]
            }
          }
        }
      ]
    }
  },
  
  // Kombinuj rezultate i filtriraj
  {
    $addFields: {
      global_fraud_rate: {$arrayElemAt: ["$global_stats.global_fraud_rate", 0]},
      threshold: {
        $multiply: [
          {$arrayElemAt: ["$global_stats.global_fraud_rate", 0]}, 
          2
        ]
      }
    }
  },
  
  // Unwind location analizu
  {$unwind: "$location_analysis"},
  
  // Filtriraj lokacije sa visokim fraud rate-om i minimum transakcija
  {
    $match: {
      $expr: {
        $and: [
          {$gte: ["$location_analysis.fraud_rate", "$threshold"]},
          {$gte: ["$location_analysis.total_transactions", 50]}, // Minimum 50 transakcija
          {$gt: ["$location_analysis.fraud_transactions", 0]}
        ]
      }
    }
  },
  
  // Dodaj dodatne kalkulacije
  {
    $addFields: {
      fraud_multiplier: {
        $round: [
          {$divide: ["$location_analysis.fraud_rate", "$global_fraud_rate"]},
          2
        ]
      },
      fraud_percentage: {
        $round: [
          {$multiply: ["$location_analysis.fraud_rate", 100]},
          2
        ]
      }
    }
  },
  
  // Sortiraj po fraud rate-u
  {$sort: {"location_analysis.fraud_rate": -1}},
  
  // Ograniči na top 5
  {$limit: 5},
  
  // Formatuj rezultat
  {
    $project: {
      _id: 0,
      location: {
        city: "$location_analysis._id.city",
        state: "$location_analysis._id.state"
      },
      age_group: "$location_analysis._id.age_group",
      statistics: {
        total_transactions: "$location_analysis.total_transactions",
        fraud_transactions: "$location_analysis.fraud_transactions",
        fraud_percentage: "$fraud_percentage",
        avg_fraud_amount: {$round: ["$location_analysis.avg_fraud_amount", 2]},
        fraud_multiplier: "$fraud_multiplier"
      },
      global_comparison: {
        global_fraud_rate: {
          $round: [
            {$multiply: ["$global_fraud_rate", 100]},
            2
          ]
        },
        times_above_global: "$fraud_multiplier"
      }
    }
  }
])
```

## Očekivani Rezultati

Upit će mi pokazati **top 5 najrizičnijih lokacija** po starosnim grupama. Na primer:

- **Miami, FL** - starosna grupa 25-34: 2.8% fraud rate (4.8x iznad proseka)

**Napomena:**
Izvršavanje trajalo 3:07m
 Upit koristi minimum od 50 transakcija po kombinaciji grada/države/starosne grupe da izbegne statistički neznačajne rezultate.

### Geografska Distribucija
![Geographic Hotspots](geographic.jpg)

---

*Miloš - Geografska Analiza Prevara - Oktobar 2025*