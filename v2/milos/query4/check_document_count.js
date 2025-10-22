// Provera koliko dokumenata se koristi u Query 4

// 1. Total broj dokumenata u kolekciji
db.transactions_enriched.countDocuments()

// 2. Broj fraud dokumenata (što Query 4 koristi)
db.transactions_enriched.countDocuments({ is_fraud_num: 1 })

// 3. Detaljna statistika sa explain()
db.transactions_enriched.aggregate([
  {
    $match: {
      is_fraud_num: 1
    }
  },
  {
    $count: "total_fraud_documents"
  }
])

// 4. Explain plan da vidiš da li koristi index
// explain() ide PRE aggregate()!
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

// 5. Ili koristi allPlansExecution za detaljnu analizu
db.transactions_enriched.explain("allPlansExecution").aggregate([
  {
    $match: {
      is_fraud_num: 1
    }
  }
])
