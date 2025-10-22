[
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
    $group: {
      _id: "$card.card_provider",
      total_transactions: {$sum: 1},
      fraud_transactions: {$sum: {$toInt: "$is_fraud"}}
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
          {$multiply: [
            {$divide: ["$fraud_transactions", "$total_transactions"]}, 
            100
          ]},
          2
        ]
      }
    }
  },
  
  {$sort: {fraud_percentage: -1}}
]