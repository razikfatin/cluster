from pymongo import MongoClient, WriteConcern
import time

# --- Connection URIs for each node ---
PRIMARY_URI = "mongodb://localhost:27017/?directConnection=true"
SECONDARY1_URI = "mongodb://localhost:27018/?directConnection=true"
SECONDARY2_URI = "mongodb://localhost:27019/?directConnection=true"

REPLICA_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
DB_NAME = "replicaTestDB"
COLL_NAME = "durabilityFinal"

# --- Connect to replica set and each node ---
client = MongoClient(REPLICA_URI)
db = client[DB_NAME]
coll = db[COLL_NAME]
coll.drop()

primary = MongoClient(PRIMARY_URI)
secondary1 = MongoClient(SECONDARY1_URI)
secondary2 = MongoClient(SECONDARY2_URI)

print("\n✅ Connected to all nodes (Primary + 2 Secondaries)\n")

# --- Helper to print documents from each node ---
def show_docs(node_client, label):
    docs = list(node_client[DB_NAME][COLL_NAME].find({}, {"_id": 1, "wc": 1}).sort("_id", 1))
    ids = [d["_id"] for d in docs]
    print(f"{label}: {len(docs)} docs → {ids}")
    return set(ids)

# --- Test different write concerns ---
tests = [
    ("w:1", WriteConcern(w=1)),
    ("w:majority", WriteConcern(w="majority", j=True)),
    ("w:3", WriteConcern(w=3, j=True))
]

for label, wc in tests:
    print(f"\n🧩 Testing {label}")
    coll.drop()  # clear previous round’s documents
    wc_coll = coll.with_options(write_concern=wc)

    # --- Insert 10 docs (IDs 1–10) ---
    for i in range(1, 11):
        wc_coll.insert_one({"_id": i, "wc": label})
    print(f"Inserted 10 docs with {label}: IDs [1–10]")

    # --- Immediate check ---
    print("\n📊 Immediately after inserts:")
    ids_primary = show_docs(primary, "Primary")
    ids_s1 = show_docs(secondary1, "Secondary 1")
    ids_s2 = show_docs(secondary2, "Secondary 2")

    # --- Missing IDs check ---
    missing_s1 = sorted(list(ids_primary - ids_s1))
    missing_s2 = sorted(list(ids_primary - ids_s2))
    if missing_s1 or missing_s2:
        print(f"⚠️ Missing on secondaries right after insert →")
        if missing_s1:
            print(f"   Secondary 1 missing: {missing_s1}")
        if missing_s2:
            print(f"   Secondary 2 missing: {missing_s2}")
    else:
        print("✅ All docs already replicated to both secondaries.")

    # --- Wait and recheck after replication delay ---
    print("\n⏱ Waiting 5 seconds for replication to catch up...\n")
    time.sleep(5)

    ids_s1_after = show_docs(secondary1, "Secondary 1 (after 5s)")
    ids_s2_after = show_docs(secondary2, "Secondary 2 (after 5s)")

    if ids_primary == ids_s1_after == ids_s2_after:
        print("✅ All nodes now consistent.")
    else:
        print("⚠️ Still some differences remain after 5s.")

    # --- Clean up for next test ---
    print("\n🧹 Deleting all documents before next test...\n")
    coll.delete_many({})

print("\n✅ Durability & replication test complete.")
print("Observe: w:1 shows delay in replication, while w:majority and w:3 are consistent immediately.")
