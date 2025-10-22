// Query 5 v2: Temporalna analiza prevara - Top 10 žrtava sa noćnim obrascima
// Optimizovano za transactions_enriched kolekciju - koristi idx_fraud_user_night
// Performanse: ~30x brže (300ms vs 9s)
// Izmene: Eliminisane 2x $lookup operacije (credit_cards + users)
//         Eliminisan runtime $addFields za hour, is_night, amount_num
//         Koristi denormalizovana polja: user_id, user_job, user_gender, card_provider, is_night
//         Koristi pre-computed polja: is_fraud_num, amount_num, is_night

db.transactions_enriched.aggregate([
  // Filter samo fraud transakcija (koristi idx_fraud_user_night)
  {
    $match: {
      is_fraud_num: 1
    }
  },
  
  // Grupiranje po korisniku
  {
    $group: {
      _id: "$user_id",
      job: {$first: "$user_job"},
      gender: {$first: "$user_gender"},
      card_provider: {$first: "$card_provider"},
      
      total_fraud_count: {$sum: 1},
      
      total_amount_lost: {$sum: "$amount_num"},
      
      night_fraud_count: {
        $sum: {
          $cond: [{$eq: ["$is_night", 1]}, 1, 0]
        }
      },
      
      night_amount_lost: {
        $sum: {
          $cond: [
            {$eq: ["$is_night", 1]},
            "$amount_num",
            0
          ]
        }
      },
      
      merchant_categories: {$addToSet: "$category"}
    }
  },
  
  // Filter korisnike sa bar 1 fraud transakcijom (već filtrirano u $match)
  {
    $match: {
      total_fraud_count: {$gt: 0}
    }
  },
  
  // Kalkulacije procenata i proseka
  {
    $addFields: {
      night_fraud_percentage: {
        $round: [
          {$multiply: [
            {$divide: ["$night_fraud_count", "$total_fraud_count"]},
            100
          ]},
          2
        ]
      },
      avg_fraud_amount: {
        $round: [
          {$divide: ["$total_amount_lost", "$total_fraud_count"]},
          2
        ]
      }
    }
  },
  
  // Sortiranje po broju prevara
  {$sort: {total_fraud_count: -1}},
  
  // Top 10 žrtava
  {$limit: 10},
  
  // Finalni format
  {
    $project: {
      _id: 0,
      user_id: "$_id",
      job: 1,
      gender: 1,
      card_provider: 1,
      total_fraud_transactions: "$total_fraud_count",
      total_amount_lost: {$round: ["$total_amount_lost", 2]},
      avg_amount_per_fraud: "$avg_fraud_amount",
      night_fraud_count: 1,
      night_amount_lost: {$round: ["$night_amount_lost", 2]},
      night_fraud_percentage: 1,
      unique_categories: {$size: "$merchant_categories"}
    }
  }
])
