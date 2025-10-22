# Query 4: Profil Žrtava - Posao i Kategorija Trgovca

## Šta ovaj upit radi?

Dva problema u jednom:
1. **Top 15 kombinacija posao + kategorija trgovca** sa najviše prevara
2. **Top 10 najugroženijih profesija** sa demografskim podacima (pol)

Odgovara na pitanje: Ko su najčešće žrtve i gde se prevare dešavaju?

## Rezultati

![Query 4 Results Part 1](output1.png)

![Query 4 Results Part 2](output2.png)

![Performace](performance.png)


## MongoDB Kod

```javascript
db.transactions_enriched.aggregate([
  {
    $match: {
      is_fraud_num: 1
    }
  },
  {
    $facet: {
      "job_category_analysis": [
        {
          $group: {
            _id: {
              job: "$user_job",
              category: "$category"
            },
            total_transactions: {$sum: 1},
            fraud_transactions: {$sum: 1},
            unique_victims: {$addToSet: "$user_id"}
          }
        },
        {
          $addFields: {
            fraud_rate: 100.0,
            victim_count: {$size: "$unique_victims"}
          }
        },
        {$match: {fraud_transactions: {$gte: 10}}},
        {$sort: {fraud_transactions: -1}},
        {$limit: 15},
        {
          $project: {
            job: "$_id.job",
            category: "$_id.category",
            fraud_count: "$fraud_transactions",
            fraud_rate: 1,
            victim_count: 1,
            _id: 0
          }
        }
      ],
      "top_vulnerable_jobs": [
        {
          $group: {
            _id: "$user_job",
            total_fraud: {$sum: 1},
            unique_victims: {$addToSet: "$user_id"},
            genders: {$push: "$user_gender"}
          }
        },
        {$sort: {total_fraud: -1}},
        {$limit: 10},
        {
          $project: {
            profession: "$_id",
            total_fraud_count: "$total_fraud",
            unique_victim_count: {$size: "$unique_victims"},
            gender_split: {
              $reduce: {
                input: "$genders",
                initialValue: {},
                in: {
                  $mergeObjects: [
                    "$$value",
                    {
                      $arrayToObject: [[{
                        k: "$$this",
                        v: {$add: [
                          {$ifNull: [{$getField: {field: "$$this", input: "$$value"}}, 0]},
                          1
                        ]}
                      }]]
                    }
                  ]
                }
              }
            },
            _id: 0
          }
        }
      ]
    }
  },
  {
    $project: {
      top_risky_job_category_combinations: "$job_category_analysis",
      most_vulnerable_professions: "$top_vulnerable_jobs"
    }
  }
])
```

