db.transactions_enriched.aggregate([
  // No $lookup needed - card_provider is denormalized!

  {
    $group: {
      _id: "$card_provider",
      total_transactions: { $sum: 1 },
      fraud_transactions: { $sum: "$is_fraud_num" }  // Pre-converted to int
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
