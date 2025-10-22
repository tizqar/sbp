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
  
  {
    $group: {
      _id: {
        city: "$location.city",
        state: "$location.state"
      },
      total_transactions: {$sum: 1},
      fraud_transactions: {$sum: {$toInt: "$is_fraud"}}
    }
  },
  
  {
    $project: {
      _id: 0,
      city: "$_id.city",
      state: "$_id.state",
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
  
  {$sort: {fraud_transactions: -1}},
  
  {$limit: 10}
])