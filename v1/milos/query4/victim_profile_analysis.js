// Analiza TOP 10 žrtava prevara - profil i obrasci
db.transactions.aggregate([
  // Spoji sa karticama
  {
    $lookup: {
      from: "credit_cards",
      localField: "credit_card_id", 
      foreignField: "_id",
      as: "card"
    }
  },
  {$unwind: "$card"},
  
  // Spoji sa korisnicima
  {
    $lookup: {
      from: "users",
      localField: "card.user_id", 
      foreignField: "_id",
      as: "user"
    }
  },
  {$unwind: "$user"},
  
  // Spoji sa lokacijama za demografske podatke
  {
    $lookup: {
      from: "locations",
      localField: "user.location_id", 
      foreignField: "_id",
      as: "location"
    }
  },
  {$unwind: "$location"},
  
  // Konvertuj potrebna polja
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
  
  // Kalkuliši uzrast
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
  
  // Grupiši po kreditnoj kartici (cc_num)
  {
    $group: {
      _id: "$card.cc_num",
      // Demografski podaci (uzmi prvi - svi su isti za istu karticu)
      user_profile: {$first: {
        name: {$concat: ["$user.first", " ", "$user.last"]},
        gender: "$user.gender",
        age: "$user_age",
        age_group: "$age_group",
        job: "$user.job",
        dob: "$user.dob",
        location: {
          city: "$location.city",
          state: "$location.state"
        }
      }},
      card_provider: {$first: "$card.card_provider"},
      
      // Statistike transakcija
      total_transactions: {$sum: 1},
      fraud_transactions: {$sum: "$is_fraud_num"},
      normal_transactions: {
        $sum: {
          $cond: [
            {$eq: ["$is_fraud_num", 0]}, 
            1, 
            0
          ]
        }
      },
      
      // Finansijski gubici
      total_amount_all: {$sum: "$amount_num"},
      total_fraud_amount: {
        $sum: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]}, 
            "$amount_num", 
            0
          ]
        }
      },
      
      // Kategorije gde se prevare dešavaju
      fraud_categories: {
        $push: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]},
            {
              category: "$category",
              amount: "$amount_num",
              date: "$trans_date_trans_time"
            },
            "$$REMOVE"
          ]
        }
      },
      
      // Prva i poslednja prevara
      first_fraud_date: {
        $min: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]}, 
            "$trans_date_trans_time", 
            null
          ]
        }
      },
      last_fraud_date: {
        $max: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]}, 
            "$trans_date_trans_time", 
            null
          ]
        }
      }
    }
  },
  
  // Filtriraj samo korisnike sa prevarama
  {
    $match: {
      fraud_transactions: {$gt: 0}
    }
  },
  
  // Analiziraj kategorije prevara
  {
    $addFields: {
      // Grupiši prevare po kategorijama
      category_analysis: {
        $reduce: {
          input: "$fraud_categories",
          initialValue: {},
          in: {
            $mergeObjects: [
              "$$value",
              {
                $arrayToObject: [
                  [{
                    k: "$$this.category",
                    v: {
                      $add: [
                        {$ifNull: [{$getField: {field: "$$this.category", input: "$$value"}}, 0]},
                        1
                      ]
                    }
                  }]
                ]
              }
            ]
          }
        }
      },
      
      // Kalkuliši proseke
      avg_fraud_amount: {
        $cond: [
          {$gt: ["$fraud_transactions", 0]},
          {$divide: ["$total_fraud_amount", "$fraud_transactions"]},
          0
        ]
      },
      fraud_percentage: {
        $round: [
          {$multiply: [
            {$divide: ["$fraud_transactions", "$total_transactions"]}, 
            100
          ]},
          2
        ]
      }
    }
  },
  
  // Sortiraj po broju prevara (descending)
  {$sort: {fraud_transactions: -1}},
  
  // Ograniči na TOP 10
  {$limit: 10},
  
  // Dodaj ranking
  {
    $group: {
      _id: null,
      victims: {
        $push: {
          cc_num: "$_id",
          rank: {$add: [{$size: "$victims"}, 1]},
          user_profile: "$user_profile",
          card_provider: "$card_provider",
          fraud_statistics: {
            total_fraud_count: "$fraud_transactions",
            total_fraud_amount: {$round: ["$total_fraud_amount", 2]},
            avg_fraud_amount: {$round: ["$avg_fraud_amount", 2]},
            fraud_percentage: "$fraud_percentage",
            total_transactions: "$total_transactions"
          },
          fraud_timeline: {
            first_fraud: "$first_fraud_date",
            last_fraud: "$last_fraud_date",
            fraud_period_days: {
              $cond: [
                {$and: ["$first_fraud_date", "$last_fraud_date"]},
                {
                  $divide: [
                    {$subtract: [
                      {$dateFromString: {dateString: "$last_fraud_date"}},
                      {$dateFromString: {dateString: "$first_fraud_date"}}
                    ]},
                    86400000
                  ]
                },
                0
              ]
            }
          },
          top_fraud_categories: "$fraud_categories"
        }
      }
    }
  },
  
  // Dodaj ranking brojeve
  {
    $addFields: {
      victims: {
        $map: {
          input: {$range: [0, {$size: "$victims"}]},
          as: "idx",
          in: {
            $mergeObjects: [
              {$arrayElemAt: ["$victims", "$$idx"]},
              {rank: {$add: ["$$idx", 1]}}
            ]
          }
        }
      }
    }
  },
  
  // Finalni format
  {
    $project: {
      _id: 0,
      analysis_title: "TOP 10 Žrtava Prevara - Profil i Obrasci",
      total_victims_analyzed: {$size: "$victims"},
      victims: "$victims"
    }
  }
])