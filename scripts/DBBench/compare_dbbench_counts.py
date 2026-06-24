import csv
import sys
from pathlib import Path

cottas_csv = Path(sys.argv[1])
vortex_csv = Path(sys.argv[2])

def load_first_measured(path):
    out = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["phase"] != "measured":
                continue
            if row["run"] != "0":
                continue
            if row["status"] != "ok":
                continue
            out[row["query_id"]] = int(row["result_count"])
    return out

c = load_first_measured(cottas_csv)
v = load_first_measured(vortex_csv)

all_ids = sorted(set(c) | set(v))
bad = []

for qid in all_ids:
    if c.get(qid) != v.get(qid):
        bad.append((qid, c.get(qid), v.get(qid)))

print(f"cottas queries: {len(c)}")
print(f"vortex queries: {len(v)}")
print(f"mismatches: {len(bad)}")

for qid, cr, vr in bad[:50]:
    print(qid, "cottas=", cr, "vortex=", vr)

if bad:
    sys.exit(1)