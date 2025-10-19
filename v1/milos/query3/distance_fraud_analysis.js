// Analiza udaljenosti između korisnika i trgovaca - fraud pattern
db.transactions.aggregate([
  // Spoji sve potrebne kolekcije
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
      as: "user_location"
    }
  },
  {$unwind: "$user_location"},
  
  {
    $lookup: {
      from: "merchants",
      localField: "merchant_id", 
      foreignField: "_id",
      as: "merchant"
    }
  },
  {$unwind: "$merchant"},
  
  // Konvertuj potrebna polja
  {
    $addFields: {
      is_fraud_num: {$toInt: "$is_fraud"},
      amount_num: {$toDouble: "$amt"},
      user_lat: {$toDouble: "$user_location.lat"},
      user_lng: {$toDouble: "$user_location.long"},
      merchant_lat: {$toDouble: "$merchant.merch_lat"},
      merchant_lng: {$toDouble: "$merchant.merch_long"}
    }
  },
  
  // Filtriraj samo validne koordinate
  {
    $match: {
      user_lat: {$ne: null, $ne: 0},
      user_lng: {$ne: null, $ne: 0},
      merchant_lat: {$ne: null, $ne: 0},
      merchant_lng: {$ne: null, $ne: 0}
    }
  },
  
  // Kalkuliši udaljenost pomoću Haversine formule (aproksimacija)
  {
    $addFields: {
      lat_diff: {$subtract: ["$merchant_lat", "$user_lat"]},
      lng_diff: {$subtract: ["$merchant_lng", "$user_lng"]},
      
      // Haversine formula - aproksimacija za udaljenost u km
      distance_km: {
        $multiply: [
          111, // Prosečna udaljenost u km po stepenu
          {
            $sqrt: {
              $add: [
                {$pow: [{$subtract: ["$merchant_lat", "$user_lat"]}, 2]},
                {
                  $multiply: [
                    {$pow: [{$subtract: ["$merchant_lng", "$user_lng"]}, 2]},
                    {
                      $pow: [
                        {$cos: {$multiply: [{$divide: ["$user_lat", 57.2958]}, 1]}}, 
                        2
                      ]
                    }
                  ]
                }
              ]
            }
          }
        ]
      }
    }
  },
  
  // Kategoriziraj po udaljenosti
  {
    $addFields: {
      distance_category: {
        $switch: {
          branches: [
            { case: { $lte: ["$distance_km", 10] }, then: "Local (0-10km)" },
            { case: { $lte: ["$distance_km", 50] }, then: "Regional (10-50km)" },
            { case: { $lte: ["$distance_km", 100] }, then: "Distant (50-100km)" },
            { case: { $lte: ["$distance_km", 500] }, then: "Far (100-500km)" }
          ],
          default: "Very Far (500km+)"
        }
      },
      is_long_distance: {
        $cond: [
          {$gt: ["$distance_km", 100]}, 
          1, 
          0
        ]
      }
    }
  },
  
  // Grupiši po card provider-u i kategoriji udaljenosti
  {
    $group: {
      _id: {
        card_provider: "$card.card_provider",
        distance_category: "$distance_category",
        is_long_distance: "$is_long_distance"
      },
      total_transactions: {$sum: 1},
      fraud_transactions: {$sum: "$is_fraud_num"},
      total_amount: {$sum: "$amount_num"},
      fraud_amount: {
        $sum: {
          $cond: [
            {$eq: ["$is_fraud_num", 1]}, 
            "$amount_num", 
            0
          ]
        }
      },
      avg_distance: {$avg: "$distance_km"},
      max_distance: {$max: "$distance_km"}
    }
  },
  
  // Kalkuliši procente
  {
    $addFields: {
      fraud_percentage: {
        $round: [
          {$multiply: [
            {$divide: ["$fraud_transactions", "$total_transactions"]}, 
            100
          ]},
          2
        ]
      },
      avg_fraud_amount: {
        $cond: [
          {$gt: ["$fraud_transactions", 0]},
          {$round: [{$divide: ["$fraud_amount", "$fraud_transactions"]}, 2]},
          0
        ]
      }
    }
  },
  
  // Fokus na long distance transakcije (>100km)
  {
    $match: {
      "_id.is_long_distance": 1
    }
  },
  
  // Grupiši po card provider-u za finalni izveštaj
  {
    $group: {
      _id: "$_id.card_provider",
      total_long_distance_transactions: {$sum: "$total_transactions"},
      total_long_distance_fraud: {$sum: "$fraud_transactions"},
      total_long_distance_amount: {$sum: "$total_amount"},
      total_long_distance_fraud_amount: {$sum: "$fraud_amount"},
      avg_distance_all: {
        $avg: "$avg_distance"
      },
      max_distance_observed: {$max: "$max_distance"},
      distance_breakdown: {
        $push: {
          category: "$_id.distance_category",
          transactions: "$total_transactions",
          fraud_count: "$fraud_transactions",
          fraud_percentage: "$fraud_percentage",
          avg_distance_km: {$round: ["$avg_distance", 2]},
          avg_fraud_amount: "$avg_fraud_amount"
        }
      }
    }
  },
  
  // Kalkuliši ukupne procente
  {
    $addFields: {
      overall_long_distance_fraud_percentage: {
        $round: [
          {$multiply: [
            {$divide: ["$total_long_distance_fraud", "$total_long_distance_transactions"]}, 
            100
          ]},
          2
        ]
      },
      avg_fraud_amount_overall: {
        $cond: [
          {$gt: ["$total_long_distance_fraud", 0]},
          {$round: [
            {$divide: ["$total_long_distance_fraud_amount", "$total_long_distance_fraud"]}, 
            2
          ]},
          0
        ]
      }
    }
  },
  
  // Sortiraj po fraud procentu
  {$sort: {"overall_long_distance_fraud_percentage": -1}},
  
  // Formatuj finalni rezultat
  {
    $project: {
      _id: 0,
      card_provider: "$_id",
      long_distance_analysis: {
        total_transactions: "$total_long_distance_transactions",
        fraud_transactions: "$total_long_distance_fraud",
        fraud_percentage: "$overall_long_distance_fraud_percentage",
        avg_distance_km: {$round: ["$avg_distance_all", 2]},
        max_distance_km: {$round: ["$max_distance_observed", 2]},
        avg_fraud_amount: "$avg_fraud_amount_overall"
      },
      distance_category_breakdown: "$distance_breakdown"
    }
  }
])