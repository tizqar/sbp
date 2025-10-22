
db.transactions_enriched.aggregate([
  // No $lookup needed - user_city is denormalized!
  
  // NOTE: This query analyzes ALL transactions (fraud + non-fraud)
  // If you want only fraud transactions, add: { $match: { is_fraud_num: 1 } }
  
  {
    $group: {
      _id: "$user_city",
      total_transactions: { $sum: 1 },
      fraud_transactions: { $sum: "$is_fraud_num" }  // Pre-converted to int
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
  
  // Filter out null/empty cities AFTER grouping
  {
    $match: {
      city: { $ne: null, $ne: "" }
    }
  },
  
  { $sort: { fraud_transactions: -1 } },
  
  { $limit: 10 }
])
