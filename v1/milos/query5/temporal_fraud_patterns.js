// Temporalna analiza prevara - Top 10 žrtava sa lokacijom i noćnim obrascima
db.transactions.aggregate([
  // Join sa credit_cards
  {
    $lookup: {
      from: "credit_cards",
      localField: "credit_card_id",
      foreignField: "_id",
      as: "card"
    }
  },
  {$unwind: "$card"},
  
  // Join sa users
  {
    $lookup: {
      from: "users",
      localField: "card.user_id",
      foreignField: "_id",
      as: "user"
    }
  },
  {$unwind: "$user"},
  
  // Ekstrakcija temporalnih podataka
  {
    $addFields: {
      amount_num: {$toDouble: "$amt"},
      hour: {$toInt: {$substr: ["$trans_date_trans_time", 11, 2]}},
      is_fraud_num: {$toInt: "$is_fraud"},
      is_night: {
        $cond: [
          {$gte: [{$toInt: {$substr: ["$trans_date_trans_time", 11, 2]}}, 18]},
          1,
          0
        ]
      }
    }
  },
  
  // Grupiranje po korisniku
  {
    $group: {
      _id: "$user._id",
      full_name: {$first: {$concat: ["$user.first_name", " ", "$user.last_name"]}},
      job: {$first: "$user.job"},
      gender: {$first: "$user.gender"},
      card_provider: {$first: "$card.card_provider"},
      
      total_fraud_count: {
        $sum: {
          $cond: [{$eq: ["$is_fraud_num", 1]}, 1, 0]
        }
      },
      total_amount_lost: {
        $sum: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]},
            "$amount_num",
            0
          ]
        }
      },
      
      night_fraud_count: {
        $sum: {
          $cond: [
            {$and: [
              {$eq: ["$is_fraud_num", 1]},
              {$eq: ["$is_night", 1]}
            ]},
            1,
            0
          ]
        }
      },
      night_amount_lost: {
        $sum: {
          $cond: [
            {$and: [
              {$eq: ["$is_fraud_num", 1]},
              {$eq: ["$is_night", 1]}
            ]},
            "$amount_num",
            0
          ]
        }
      },
      
      merchant_categories: {
        $addToSet: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]},
            "$category",
            null
          ]
        }
      }
    }
  },
  
  // Filtriraj korisnike koji imaju bar jednu fraud transakciju
  {
    $match: {
      total_fraud_count: {$gt: 0}
    }
  },
  
  // Očisti null iz kategorija
  {
    $addFields: {
      merchant_categories: {
        $filter: {
          input: "$merchant_categories",
          cond: {$ne: ["$$this", null]}
        }
      }
    }
  },
  
  // Kalkulacije procenata
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
  
  // Top 10
  {$limit: 10},
  
  // Finalni format
  {
    $project: {
      _id: 0,
      user_id: "$_id",
      full_name: 1,
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
