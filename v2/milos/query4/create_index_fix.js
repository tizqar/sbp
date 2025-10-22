// Fix: Kreiranje nedostajućih indexa za Query 4

// 1. Proveri postojeće indexe
db.transactions_enriched.getIndexes()

// 2. PROBLEM: Indexi koriste is_fraud (string), a query koristi is_fraud_num (int)!
// REŠENJE: Kreiraj nove indexe sa is_fraud_num

// Drop stari index
db.transactions_enriched.dropIndex("idx_fraud_job_category")

// Kreiraj novi index sa is_fraud_num
db.transactions_enriched.createIndex(
  {
    is_fraud_num: 1,
    user_job: 1,
    category: 1
  },
  { 
    name: "idx_fraud_job_category_num",
    background: true 
  }
)

// 3. Verifikuj da je index kreiran
db.transactions_enriched.getIndexes()

// 4. Test query sa explain() da vidiš da sada koristi index
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

// Očekivani rezultat POSLE kreiranja indexa:
// - totalKeysExamined: 7506 (broj fraud dokumenata)
// - totalDocsExamined: 7506 (samo fraud dokumenti)
// - indexName: "idx_fraud_job_category" ✅
