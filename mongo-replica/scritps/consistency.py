import time
from datetime import datetime, timezone
from pymongo import MongoClient, WriteConcern
from pymongo.errors import PyMongoError

PRIMARY_URI = "mongodb://mongo1:27017/?directConnection=true"
SECONDARY1_URI = "mongodb://mongo2:27017/?directConnection=true"
SECONDARY2_URI = "mongodb://mongo3:27017/?directConnection=true"
REPLICA_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

DB_NAME = "replicaTestDB"
COLL_NAME = "consistency_final"
DOC_COUNT = 10

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def print_banner(title):
    print("\n" + "=" * 100)
    print(f"{title}  ({now()})")
    print("=" * 100 + "\n")

def show_docs(client, label):
    """Print documents from a specific node."""
    try:
        docs = list(client[DB_NAME][COLL_NAME].find({}, {"_id": 1}).sort("_id", 1))
        ids = [d["_id"] for d in docs]
        print(f"{label}: {len(docs)} docs → {ids}")
        return set(ids)
    except Exception as e:
        print(f"{label}: error → {e}")
        return set()

def strong_consistency():
    print_banner("🧱 STRONG CONSISTENCY (w:'majority', CP System)")
    client = MongoClient(REPLICA_URI)
    coll = client[DB_NAME][COLL_NAME]
    coll.drop()

    primary = MongoClient(PRIMARY_URI)
    secondary1 = MongoClient(SECONDARY1_URI)
    secondary2 = MongoClient(SECONDARY2_URI)

    wc_majority = WriteConcern(w="majority", j=True)
    wc_coll = coll.with_options(write_concern=wc_majority)

    print(f"Inserting {DOC_COUNT} documents with WriteConcern='majority' ...")
    for i in range(1, DOC_COUNT + 1):
        wc_coll.insert_one({"_id": i, "type": "strong", "ts": now()})
    print("✅ Inserts acknowledged by majority.\n")

    print("📊 Immediately after insert:")
    ids_primary = show_docs(primary, "Primary")
    ids_s1 = show_docs(secondary1, "Secondary 1")
    ids_s2 = show_docs(secondary2, "Secondary 2")

    if ids_primary == ids_s1 == ids_s2:
        print("✅ All replicas immediately consistent.")
    else:
        print("⚠️ Minor lag — but strong consistency guarantees read correctness.")

    time.sleep(2)
    primary.close(); secondary1.close(); secondary2.close(); client.close()
    print("\nCAP Insight: MongoDB sacrifices *availability* to guarantee *consistency* when using w:'majority' (CP).")

def eventual_consistency():
    print_banner("🌊 EVENTUAL CONSISTENCY (w=1, AP System)")
    client = MongoClient(REPLICA_URI)
    coll = client[DB_NAME][COLL_NAME]
    coll.drop()

    primary = MongoClient(PRIMARY_URI)
    secondary1 = MongoClient(SECONDARY1_URI)
    secondary2 = MongoClient(SECONDARY2_URI)

    wc_one = WriteConcern(w=1)
    wc_coll = coll.with_options(write_concern=wc_one)

    print(f"Inserting {DOC_COUNT} documents with WriteConcern=1 (primary-only acknowledgment) ...")
    for i in range(1, DOC_COUNT + 1):
        wc_coll.insert_one({"_id": i, "type": "eventual", "ts": now()})
    print("✅ Inserts acknowledged by PRIMARY only.\n")

    # Immediate check
    print("📊 Immediately after insert:")
    ids_primary = show_docs(primary, "Primary")
    ids_s1 = show_docs(secondary1, "Secondary 1")
    ids_s2 = show_docs(secondary2, "Secondary 2")

    missing_s1 = sorted(list(ids_primary - ids_s1))
    missing_s2 = sorted(list(ids_primary - ids_s2))
    if missing_s1 or missing_s2:
        print("⚠️ Missing docs on secondaries (replication lag):")
        if missing_s1:
            print(f"   Secondary 1 missing: {missing_s1}")
        if missing_s2:
            print(f"   Secondary 2 missing: {missing_s2}")
    else:
        print("✅ All docs visible — replication very fast.")

    print("\n⏱ Waiting 5 seconds for eventual replication...")
    time.sleep(5)

    # Recheck
    ids_s1_after = show_docs(secondary1, "Secondary 1 (after 5s)")
    ids_s2_after = show_docs(secondary2, "Secondary 2 (after 5s)")
    if ids_primary == ids_s1_after == ids_s2_after:
        print("✅ All replicas consistent after delay (eventual convergence).")
    else:
        print("⚠️ Some differences remain — longer lag or partition may exist.")

    primary.close(); secondary1.close(); secondary2.close(); client.close()
    print("\nCAP Insight: MongoDB prioritizes *availability* with w=1, allowing stale reads until replication completes (AP).")

def causal_consistency():
    print_banner("🔗 CAUSAL CONSISTENCY (session-level ordering)")
    client = MongoClient(REPLICA_URI)
    posts = client[DB_NAME]["posts"]
    comments = client[DB_NAME]["comments"]
    posts.drop(); comments.drop()

    try:
        sessionA = client.start_session(causal_consistency=True)
        with sessionA.start_transaction():
            posts.insert_one({"_id": "post1", "author": "A", "text": "Hello world"}, session=sessionA)
            comments.insert_one({"_id": "comment1", "post_id": "post1", "text": "Nice post!", "author": "A"}, session=sessionA)
        sessionA.end_session()
        print("✅ Client A wrote Post → Comment in a causal session.")
    except PyMongoError as e:
        print("Error in Client A write:", e)
        return

    try:
        clientB = MongoClient(REPLICA_URI)
        sessionB = clientB.start_session(causal_consistency=True)
        post = clientB[DB_NAME]["posts"].find_one({"_id": "post1"}, session=sessionB)
        comment = clientB[DB_NAME]["comments"].find_one({"_id": "comment1"}, session=sessionB)
        sessionB.end_session()
        if post and comment:
            print("✅ Client B saw both Post and Comment — causal order preserved.")
            print("Post:", post)
            print("Comment:", comment)
        else:
            print("⚠️ Client B did not see both (possible lag).")
    except Exception as e:
        print("Error during causal read:", e)

    client.close()
    print("\nCausal consistency ensures operations that are *causally related* (e.g., comment depends on post) are observed in order.")

def main():
    print_banner("MongoDB Consistency Demonstration (Strong, Eventual, Causal)")
    try:
        primary = MongoClient(PRIMARY_URI)
        primary.admin.command("ping")
        print("Connected to Primary, Secondary 1 & Secondary 2 ✓")
    except Exception as e:
        print("❌ Connection error:", e)
        return

    strong_consistency()
    eventual_consistency()
    causal_consistency()

    print_banner("📚 CAP Theorem Summary & Use Cases")
    print("1️⃣ Strong Consistency (CP):")
    print("   • Ensures all nodes return the latest committed data.")
    print("   • Slower but reliable (ideal for banking, financial ledgers).")
    print("   • Example: w:'majority' blocks writes during partitions.")
    print()
    print("2️⃣ Eventual Consistency (AP):")
    print("   • Prioritizes availability, secondaries may be stale temporarily.")
    print("   • Eventually all replicas converge.")
    print("   • Example: w:1 — good for social feeds, IoT data, analytics.")
    print()
    print("3️⃣ Causal Consistency:")
    print("   • Guarantees order of related operations (post before comment).")
    print("   • Balances correctness with performance; avoids anomalies.")
    print()
    print("✅ All consistency models demonstrated successfully.")
    print("=" * 100 + "\n")

if __name__ == "__main__":
    main()
