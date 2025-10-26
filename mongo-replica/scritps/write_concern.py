from pymongo import MongoClient, WriteConcern
from statistics import mean
import time

URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
RUNS = 100
DB_NAME = "replicaTestDB"
COLL_NAME = "wcDemo"

client = MongoClient(URI)
client.admin.command("ping")

status = client.admin.command("replSetGetStatus")
members = status.get("members", [])
rf = len(members)
print(f"\n✅ Replication Factor (RF): {rf}")
for m in members:
    print(f" - {m['name']}: {m['stateStr']}")

db = client[DB_NAME]
coll = db[COLL_NAME]
coll.drop()

tests = {
    "w:1": WriteConcern(w=1),
    "w:majority": WriteConcern(w="majority", j=True),
    "w:3": WriteConcern(w=3, j=True)
}

print("\n--- Write Concern Benchmark ---")
for label, wc in tests.items():
    test_coll = coll.with_options(write_concern=wc)
    latencies = []
    for i in range(RUNS):
        doc = {"i": i, "msg": "test", "wc": label}
        start = time.time()
        try:
            test_coll.insert_one(doc)
        except Exception as e:
            print(f"❌ Error on {label}: {e}")
            continue
        end = time.time()
        latencies.append((end - start) * 1000)

    if latencies:
        print(f"{label:<12} → Avg: {mean(latencies):.2f} ms | Min: {min(latencies):.2f} | Max: {max(latencies):.2f}")
    else:
        print(f"{label:<12} → No successful writes")

print("\n✅ Done! Compare the average latency for each write concern.")
print(" - w:1        → Fastest (Primary only)")
print(" - w:majority → Balanced (2 of 3 nodes)")
print(" - w:3        → Slowest (All 3 nodes confirm)")
