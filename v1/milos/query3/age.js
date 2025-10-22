// Učestalost prevara po starosnim grupama, kategorijama i polu
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
  
  // Kalkuliši starost
  {
    $addFields: {
      birth_year: {$toInt: {$substr: ["$user.dob", 0, 4]}},
      transaction_year: {$toInt: {$substr: ["$trans_date_trans_time", 0, 4]}},
      user_age: {
        $subtract: [
          {$toInt: {$substr: ["$trans_date_trans_time", 0, 4]}},
          {$toInt: {$substr: ["$user.dob", 0, 4]}}
        ]
      }
    }
  },
  
  // Dodaj starosnu grupu
  {
    $addFields: {
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
  
  // Grupiši po starosnoj grupi, kategoriji i polu
  {
    $group: {
      _id: {
        age_group: "$age_group",
        category: "$category",
        gender: "$user.gender"
      },
      total_transactions: {$sum: 1},
      fraud_transactions: {$sum: {$toInt: "$is_fraud"}}
    }
  },
  
  {
    $project: {
      _id: 0,
      age_group: "$_id.age_group",
      category: "$_id.category",
      gender: "$_id.gender",
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
  
  {$sort: {fraud_percentage: -1}},
  
  {$limit: 20}
])