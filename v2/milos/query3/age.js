
db.transactions_enriched.aggregate([
  // No $lookup needed - all fields denormalized!
  // No age calculation - user_age_group is pre-computed!
  
  // Optional: Filter only fraud to use idx_fraud_demographics_compound
  // Remove this $match if you want to see fraud_rate across all transactions
  {
    $match: {
      is_fraud_num: 1  // Uses idx_fraud_demographics_compound
    }
  },
  
  {
    $group: {
      _id: {
        age_group: "$user_age_group",  // Pre-calculated!
        category: "$category",
        gender: "$user_gender"         // Denormalized!
      },
      total_transactions: { $sum: 1 },
      fraud_transactions: { $sum: 1 }  // All are fraud due to $match
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
      fraud_rate: 100.0  // All transactions are fraud due to $match
    }
  },
  
  { $sort: { fraud_transactions: -1 } },  // Sort by count instead of rate
  
  { $limit: 20 }
])
