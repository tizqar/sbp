db.transactions_enriched.dropIndex("idx_fraud")
db.transactions_enriched.dropIndex("idx_fraud_provider")
db.transactions_enriched.dropIndex("idx_fraud_city")
db.transactions_enriched.dropIndex("idx_fraud_job_category")
db.transactions_enriched.dropIndex("idx_fraud_user_night")
db.transactions_enriched.dropIndex("idx_fraud_demographics_compound")


// Basic fraud index
db.transactions_enriched.createIndex(
  { is_fraud_num: 1 },
  { name: "idx_fraud_num" }
)

// Query 1: Fraud per card provider
db.transactions_enriched.createIndex(
  { is_fraud_num: 1, card_provider: 1 },
  { name: "idx_fraud_provider_num" }
)

// Query 2: Fraud per city
db.transactions_enriched.createIndex(
  { is_fraud_num: 1, user_city: 1 },
  { name: "idx_fraud_city_num" }
)

// Query 3: Fraud demographics (COMPOUND 4 fields)
db.transactions_enriched.createIndex(
  { is_fraud_num: 1, user_age_group: 1, category: 1, user_gender: 1 },
  { name: "idx_fraud_demographics_compound_num" }
)

// Query 4: Fraud per job and category
db.transactions_enriched.createIndex(
  { is_fraud_num: 1, user_job: 1, category: 1 },
  { name: "idx_fraud_job_category_num" }
)

// Query 5: Fraud per user with night flag
db.transactions_enriched.createIndex(
  { is_fraud_num: 1, user_id: 1, is_night: 1 },
  { name: "idx_fraud_user_night_num" }
)

// 3. Verify new indexes
db.transactions_enriched.getIndexes()

// 4. Test Query 4 again
db.transactions_enriched.explain("executionStats").aggregate([
  {
    $match: {
      is_fraud_num: 1
    }
  },
  {
    $group: {
      _id: {
        job: "$user_job",
        category: "$category"
      },
      count: {$sum: 1}
    }
  }
])

