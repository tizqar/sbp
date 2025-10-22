db.transactions_enriched.drop();
print("✓ Dropped existing transactions_enriched (if existed)\n");

// Step 2: Check transaction count
const totalTransactions = db.transactions.countDocuments();
print("Total transactions to process: " + totalTransactions + "\n");

// Step 3: Add indexes to speed up lookups
print("Creating indexes on source collections...");
db.credit_cards.createIndex({ _id: 1 });
db.credit_cards.createIndex({ user_id: 1 });
db.users.createIndex({ _id: 1 });
db.locations.createIndex({ user_id: 1 });
print("✓ Indexes created\n");

// Step 4: Run ETL pipeline
print("Starting ETL process...");
const startTime = new Date();

db.transactions.aggregate([
  {
    $lookup: {
      from: "credit_cards",
      localField: "credit_card_id",
      foreignField: "_id",
      as: "card"
    }
  },
  { $unwind: { path: "$card", preserveNullAndEmptyArrays: false } },
  
  {
    $lookup: {
      from: "users",
      localField: "card.user_id",
      foreignField: "_id",
      as: "user"
    }
  },
  { $unwind: { path: "$user", preserveNullAndEmptyArrays: false } },
  
  {
    $lookup: {
      from: "locations",
      localField: "user._id",
      foreignField: "user_id",
      as: "user_location"
    }
  },
  { $unwind: { path: "$user_location", preserveNullAndEmptyArrays: true } },
  
  {
    $addFields: {
      schema_version: 2,
      amount_num: { $cond: [{ $eq: [{ $type: "$amt" }, "string"] }, { $toDouble: "$amt" }, "$amt"] },
      is_fraud_num: { $cond: [{ $eq: [{ $type: "$is_fraud" }, "string"] }, { $toInt: "$is_fraud" }, "$is_fraud"] },
      hour: { $toInt: { $substr: ["$trans_date_trans_time", 11, 2] } },
      transaction_year: { $toInt: { $substr: ["$trans_date_trans_time", 0, 4] } },
      
      time_period: {
        $switch: {
          branches: [
            { case: { $lt: [{ $toInt: { $substr: ["$trans_date_trans_time", 11, 2] } }, 6] }, then: "Night (00-06)" },
            { case: { $lt: [{ $toInt: { $substr: ["$trans_date_trans_time", 11, 2] } }, 12] }, then: "Morning (06-12)" },
            { case: { $lt: [{ $toInt: { $substr: ["$trans_date_trans_time", 11, 2] } }, 18] }, then: "Afternoon (12-18)" },
            { case: { $lt: [{ $toInt: { $substr: ["$trans_date_trans_time", 11, 2] } }, 24] }, then: "Evening (18-24)" }
          ],
          default: "Unknown"
        }
      },
      
      is_night: { $cond: [{ $gte: [{ $toInt: { $substr: ["$trans_date_trans_time", 11, 2] }}, 18] }, 1, 0] },
      
      user_age: {
        $subtract: [
          { $toInt: { $substr: ["$trans_date_trans_time", 0, 4] } },
          { $toInt: { $substr: ["$user.dob", 0, 4] } }
        ]
      },
      
      user_age_group: {
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
      },
      
      card_provider: "$card.card_provider",
      user_id: "$user._id",
      user_job: "$user.job",
      user_gender: "$user.gender",
      user_city: "$user_location.city"
    }
  },
  
  { $project: { card: 0, user: 0, user_location: 0 } },
  
  { $out: "transactions_enriched" }
  
], { allowDiskUse: true });