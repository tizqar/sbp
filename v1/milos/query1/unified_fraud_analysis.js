db.transactions.aggregate([
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
    $addFields: {
      is_fraud_num: {$toInt: "$is_fraud"}
    }
  },
  
  {
    $facet: {
      // Analiza po card providerima
      "provider_analysis": [
        {
          $group: {
            _id: "$card.card_provider",
            total_transactions: {$sum: 1},
            fraud_transactions: {$sum: "$is_fraud_num"}
          }
        },
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
            }
          }
        },
        {
          $sort: { fraud_percentage: -1 }
        },
        {
          $project: {
            provider: "$_id",
            transactions: "$total_transactions",
            fraud_count: "$fraud_transactions",
            fraud_percentage: "$fraud_percentage",
            _id: 0
          }
        }
      ],
      
      // Analiza po kategorijama
      "category_analysis": [
        {
          $group: {
            _id: "$category",
            total_transactions: {$sum: 1},
            fraud_transactions: {$sum: "$is_fraud_num"}
          }
        },
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
            }
          }
        },
        {
          $sort: { fraud_percentage: -1 }
        },
        {
          $project: {
            category: "$_id",
            transactions: "$total_transactions",
            fraud_count: "$fraud_transactions",
            fraud_percentage: "$fraud_percentage",
            _id: 0
          }
        }
      ],
      
      // Ukupna statistika
      "overall_stats": [
        {
          $group: {
            _id: null,
            total_transactions: {$sum: 1},
            total_fraud: {$sum: "$is_fraud_num"}
          }
        },
        {
          $addFields: {
            overall_fraud_percentage: {
              $round: [
                {$multiply: [
                  {$divide: ["$total_fraud", "$total_transactions"]}, 
                  100
                ]},
                2
              ]
            }
          }
        },
        {
          $project: {
            _id: 0,
            total_transactions: "$total_transactions",
            total_fraud_transactions: "$total_fraud",
            fraud_percentage: "$overall_fraud_percentage"
          }
        }
      ]
    }
  },

  {
    $project: {
      _id: 0,
      summary: {$arrayElemAt: ["$overall_stats", 0]},
      analysis_by_provider: "$provider_analysis",
      analysis_by_category: "$category_analysis",
      timestamp: new Date()
    }
  }
])