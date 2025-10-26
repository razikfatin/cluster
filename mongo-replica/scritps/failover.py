#!/usr/bin/env python3
from pymongo import MongoClient, errors, WriteConcern
from datetime import datetime
import datetime as dt
import threading, time, subprocess, sys

REPLICA_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0&retryWrites=false"

NODE_URIS = {
    "mongo1": "mongodb://mongo1:27017/?directConnection=true",
    "mongo2": "mongodb://mongo2:27017/?directConnection=true",
    "mongo3": "mongodb://mongo3:27017/?directConnection=true",
}
PRIMARY_CONTAINER = "mongo1"
DB = "failoverDemo"
COLL = "writerDocs"

WRITER_TOTAL = 40
WRITES_BEFORE_STOP = 10
WRITE_INTERVAL = 1.0
STOP_DURATION = 6
MAJ_BATCH = 20

def now_iso(): return datetime.utcnow().isoformat()

def connect(uri, timeout_ms=3000):
    c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=timeout_ms, socketTimeoutMS=timeout_ms)
    c.admin.command("ping")
    return c

def get_primary_name(client):
    try:
        st = client.admin.command("replSetGetStatus")
        for m in st.get("members", []):
            if m.get("stateStr") == "PRIMARY":
                return m.get("name")
    except Exception:
        return None

def list_seqs(uri):
    try:
        c = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000)
        docs = list(c[DB][COLL].find({}, {"seq":1}).sort("seq",1))
        return [d["seq"] for d in docs if "seq" in d]
    except Exception:
        return None

def stop_container(name):
    try:
        subprocess.run(["docker", "stop", name], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        raise RuntimeError("docker CLI not found in environment. Run script on host or use an image with docker client.") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"docker stop failed: {e.stderr.decode().strip()}") from e

def start_container(name):
    try:
        subprocess.run(["docker", "start", name], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError as e:
        raise RuntimeError("docker CLI not found in environment. Run script on host or use an image with docker client.") from e
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"docker start failed: {e.stderr.decode().strip()}") from e

class Writer(threading.Thread):
    def __init__(self, uri, total, interval=1.0, wc=None):
        super().__init__(daemon=True)
        self.client = MongoClient(uri, retryWrites=False, serverSelectionTimeoutMS=5000, socketTimeoutMS=5000)
        self.total = total
        self.interval = interval
        self.wc = wc
        self.records = []  # (seq, iso_ts, success_bool, err_msg_or_None)
        self._stop = threading.Event()

    def run(self):
        coll = self.client[DB][COLL]
        seq = 0
        while seq < self.total and not self._stop.is_set():
            ts = now_iso()
            try:
                if self.wc:
                    coll.with_options(write_concern=self.wc).insert_one({"seq": seq, "ts": ts})
                else:
                    coll.insert_one({"seq": seq, "ts": ts})
                self.records.append((seq, ts, True, None))
                print(f"{ts} | seq={seq:<3} OK")
            except errors.ServerSelectionTimeoutError as e:
                self.records.append((seq, ts, False, f"ServerSelectionTimeout: {e}"))
                print(f"{ts} | seq={seq:<3} SERVER_SELECTION_TIMEOUT -> {e}")
            except errors.AutoReconnect as e:
                self.records.append((seq, ts, False, f"AutoReconnect: {e}"))
                print(f"{ts} | seq={seq:<3} AutoReconnect -> {e}")
            except errors.WriteConcernError as e:
                self.records.append((seq, ts, False, f"WriteConcernError: {e}"))
                print(f"{ts} | seq={seq:<3} WRITE_CONCERN_ERROR -> {e}")
            except errors.NotPrimaryError as e:
                self.records.append((seq, ts, False, f"NotPrimary: {e}"))
                print(f"{ts} | seq={seq:<3} NOT_PRIMARY -> {e}")
            except Exception as e:
                self.records.append((seq, ts, False, f"{type(e).__name__}: {e}"))
                print(f"{ts} | seq={seq:<3} ERROR -> {type(e).__name__}: {e}")
            seq += 1
            slept = 0.0
            while slept < self.interval and not self._stop.is_set():
                time.sleep(0.1); slept += 0.1

    def stop(self):
        self._stop.set()

def choose_baseline(seqs_map):
    best = None; best_max = -1
    for n, s in seqs_map.items():
        if not s: v = -1
        else: v = max(s)
        if v > best_max: best_max = v; best = n
    return best

def iso_to_dt(iso_s):
    try:
        return dt.datetime.fromisoformat(iso_s)
    except Exception:
        return None

def main():
    print("\n=== Failover Experiment ===\n")
    try:
        client = connect(REPLICA_URI)
    except Exception as e:
        print("Cannot connect to replica set. Is it running and initiated?"); print(e); sys.exit(1)

    primary_before = get_primary_name(client)
    print("Primary before experiment:", primary_before)

    db = client[DB]; coll = db[COLL]; coll.drop()
    coll.insert_one({"sample": True, "ts": now_iso()})
    time.sleep(1)
    print("Sample doc counts per node:")
    for n, uri in NODE_URIS.items():
        try:
            c = connect(uri); cnt = c[DB][COLL].count_documents({"sample": True})
            print(f" - {n}: {cnt}")
        except Exception as e:
            print(f" - {n}: ERR {e}")

    # start continuous writer (w:1)
    print(f"\nStarting continuous writer (w:1) for {WRITER_TOTAL} writes...")
    writer = Writer(REPLICA_URI, WRITER_TOTAL, interval=WRITE_INTERVAL, wc=None)
    writer.start()

    # wait until writer produced WRITES_BEFORE_STOP attempts
    while len(writer.records) < WRITES_BEFORE_STOP:
        time.sleep(0.1)

    # now stop primary (destructive)
    print(f"\n*** Stopping primary container {PRIMARY_CONTAINER} NOW (destructive) ***")
    try:
        stop_container(PRIMARY_CONTAINER)
        stopped_at = now_iso()
        print("Stopped at", stopped_at)
    except Exception as e:
        print("docker stop failed:", e); writer.stop(); writer.join(); sys.exit(1)

    # detect first failure recorded by writer
    first_failure = None
    t0 = time.time()
    while time.time() - t0 < 30 and first_failure is None:
        for r in list(writer.records):
            if not r[2]:
                first_failure = r[1]; break
        time.sleep(0.1)
    print("First writer failure (if detected):", first_failure)

    # wait for election: detect new primary
    print("Waiting for new PRIMARY election (up to 60s)...")
    new_primary = None
    poll_start = time.time()
    while time.time() - poll_start < 60:
        try:
            c2 = connect(REPLICA_URI)
            np = get_primary_name(c2)
            if np and np != primary_before:
                new_primary = np; print("New primary elected:", new_primary); break
        except Exception:
            pass
        time.sleep(0.5)
    if not new_primary: print("No new primary detected within 60s.")

    # detect first success after first_failure
    first_success_after = None
    if first_failure:
        tf = iso_to_dt(first_failure)
        t1 = time.time()
        while time.time() - t1 < 60 and first_success_after is None:
            for r in list(writer.records):
                if r[2]:
                    tr = iso_to_dt(r[1])
                    if tr and tf and tr > tf:
                        first_success_after = r[1]; break
            time.sleep(0.1)
    print("First writer success after failover (if detected):", first_success_after)

    # keep stopped STOP_DURATION, then restart primary
    print(f"Keeping primary stopped for {STOP_DURATION}s ...")
    time.sleep(STOP_DURATION)
    print("Restarting primary container...")
    try:
        start_container(PRIMARY_CONTAINER); print("Restarted at", now_iso())
    except Exception as e:
        print("docker start failed:", e)
    print("Waiting 6s for cluster stabilization..."); time.sleep(6)

    # finish writer
    print("Waiting for writer to finish remaining writes...")
    while len(writer.records) < WRITER_TOTAL:
        time.sleep(0.1)

    # proper stop/join usage:
    writer.stop()
    writer.join(timeout=5)
    print("Writer finished.")

    # writer summary and downtime
    total = len(writer.records); succ = sum(1 for r in writer.records if r[2]); fail = total - succ
    print(f"\nWriter summary: attempts={total}, successes={succ}, failures={fail}")
    if first_failure: print("First failure at:", first_failure)
    if first_success_after:
        print("First success after failure at:", first_success_after)
        try:
            t_fail = iso_to_dt(first_failure); t_succ = iso_to_dt(first_success_after)
            if t_fail and t_succ:
                print("Approx downtime (s):", (t_succ - t_fail).total_seconds())
        except Exception:
            pass

    # collect per-node sequences and compare
    print("\nCollecting per-node seq lists...")
    seqs_map = {}
    for n, uri in NODE_URIS.items():
        s = list_seqs(uri)
        seqs_map[n] = s
        if s is None: print(f" - {n}: READ ERROR")
        else: print(f" - {n}: count={len(s)}  max={s[-1] if s else None}")

    baseline = choose_baseline(seqs_map)
    print("Baseline for comparison:", baseline)
    if baseline:
        base_set = set(seqs_map.get(baseline) or [])
        for n, s in seqs_map.items():
            if s is None:
                print(f" - {n}: read error")
            else:
                missing = sorted(list(base_set - set(s)))
                print(f" - {n}: missing relative to {baseline}: {missing}")

    # majority durability test (destructive)
    print("\n=== Majority durability test (destructive) ===")
    coll.drop()
    coll_maj = coll.with_options(write_concern=WriteConcern(w="majority", j=True, wtimeout=10000))
    ok = 0
    for i in range(MAJ_BATCH):
        try:
            coll_maj.insert_one({"mj_seq": i, "ts": now_iso()}); ok += 1
        except Exception as e:
            print("Maj insert error:", e)
    print(f"Inserted {ok}/{MAJ_BATCH} majority-acked docs.")

    # stop/start primary to demonstrate majority durability
    print(f"Stopping primary {PRIMARY_CONTAINER} for majority test ...")
    try:
        stop_container(PRIMARY_CONTAINER)
    except Exception as e:
        print("docker stop failed for majority test:", e)
    time.sleep(STOP_DURATION)
    try:
        start_container(PRIMARY_CONTAINER)
    except Exception as e:
        print("docker start failed for majority test:", e)
    time.sleep(5)

    # final per-node counts for majority test
    print("Final counts after majority test:")
    for n, uri in NODE_URIS.items():
        try:
            c = connect(uri); cnt = c[DB][COLL].count_documents({}); print(f" - {n}: {cnt}")
        except Exception as e:
            print(f" - {n}: ERR {e}")

    print("\nExperiment complete. Interpret as follows:")
    print(" - Downtime: first writer failure -> first success after new primary.")
    print(" - Missing seqs after destructive failover indicate writes acknowledged by old primary but not replicated (w:1 risk).")
    print(" - Majority-acked writes should survive single primary failure (no missing docs).")
    print("Done.")

if __name__ == "__main__":
    main()
