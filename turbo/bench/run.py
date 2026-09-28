#!/usr/bin/env python3
"""Benchmark harness: one subgraph, one fresh database, one graph-node process.

    run.py <case> [--bin PATH] [--ipfs URL|builtin] [--env K=V ...] [--datalake URL] [--runs N]

Per run:
  1. drop and recreate the database (cold chain cache, no call cache);
  2. launch graph-node (t0);
  3. upload the built subgraph the way `graph deploy` does (each file to /api/v0/add, the
     manifest rewritten to /ipfs/ links), then subgraph_create + subgraph_deploy;
  4. wait until the deployment has processed the case's end block (t1);
  5. digest every entity table (rows created at or below the end block, ordered by id).

Prints one JSON line per run: {"case", "seconds", "digest", "rows"}.
"""
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUBGRAPHS = HERE / "subgraphs"
PG = dict(host="localhost", port="55433", user="graph", password="graph")

CASES = {
    "usdc-transfers": dict(rpc="http://127.0.0.1:8571", end=18_699_999),
    "reth-balances": dict(rpc="http://127.0.0.1:8571", end=18_699_999),
    "allowances": dict(rpc="http://127.0.0.1:8574", end=25_601_199, features=["archive", "no_eip1898"]),
    "safe": dict(rpc="http://127.0.0.1:8573", end=24_630_000),
}
DATALAKES = {
    "usdc-transfers": "http://127.0.0.1:8084",
    "reth-balances": "http://127.0.0.1:8084",
    "allowances": "http://127.0.0.1:8082",
    "safe": "http://127.0.0.1:8083",
}


def psql(sql, db="graph", capture=False):
    env = dict(os.environ, PGPASSWORD=PG["password"])
    cmd = ["psql", "-h", PG["host"], "-p", PG["port"], "-U", PG["user"], "-d", db, "-v", "ON_ERROR_STOP=1", "-qAt", "-c", sql]
    out = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
    return out.stdout if capture else None


def fresh_db(name):
    psql(f"drop database if exists {name} with (force)")
    psql(f"create database {name} template template0 encoding 'UTF8' locale 'C'")
    psql("create extension pg_trgm; create extension btree_gist; create extension postgres_fdw; "
         "create extension pg_stat_statements", db=name)


def http(url, data=None, headers=None, method=None, timeout=30):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def ipfs_add(api, name, data):
    boundary = "----turbo" + hashlib.sha1(data).hexdigest()
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{name}\"\r\n"
            f"Content-Type: application/octet-stream\r\n\r\n").encode() + data + f"\r\n--{boundary}--\r\n".encode()
    out = http(f"{api}/api/v0/add?cid-version=0", body, {"content-type": f"multipart/form-data; boundary={boundary}"}, "POST")
    return json.loads(out.decode().strip().splitlines()[-1])["Hash"]


def upload(api, case):
    """Uploads build/ like graph-cli: every `file:` path becomes an /ipfs/ link."""
    build = SUBGRAPHS / case / "build"
    manifest = (build / "subgraph.yaml").read_text()
    cache = {}

    def link(m):
        indent, rel = m.group(1), m.group(2)
        path = (build / rel).resolve()
        if path not in cache:
            cache[path] = ipfs_add(api, path.name, path.read_bytes())
        return f"{indent}file:\n{indent}  /: /ipfs/{cache[path]}"

    manifest = re.sub(r"^(\s*)file: (\S+)$", link, manifest, flags=re.M)
    return ipfs_add(api, "subgraph.yaml", manifest.encode())


def jsonrpc(port, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    out = json.loads(http(f"http://127.0.0.1:{port}", body, {"content-type": "application/json"}, "POST"))
    if "error" in out:
        raise RuntimeError(f"{method}: {out['error']}")
    return out["result"]


def status(port, name):
    q = ('{ indexingStatusForCurrentVersion(subgraphName: "%s") { health fatalError { message } '
         'chains { latestBlock { number } } } }' % name)
    out = json.loads(http(f"http://127.0.0.1:{port}/graphql", json.dumps({"query": q}).encode(),
                          {"content-type": "application/json"}, "POST"))
    return (out.get("data") or {}).get("indexingStatusForCurrentVersion")


def digest(db, end):
    """Canonical digest of every entity table: rows alive or created at/below `end`."""
    schema = psql("select name from deployment_schemas order by id desc limit 1", db, True).strip()
    tables = psql(f"select table_name from information_schema.tables where table_schema = '{schema}' "
                  "and table_name not like 'poi2$%' order by 1", db, True).split()
    h = hashlib.sha256()
    rows = {}
    for t in tables:
        cols = psql(f"select column_name, data_type from information_schema.columns where table_schema = '{schema}' "
                    f"and table_name = '{t}' order by ordinal_position", db, True).split()
        cols = [c.split("|")[0] for c in cols]
        data = [c for c in cols if c not in ("vid", "block_range", "block$", "causality_region")]
        if "block_range" in cols:
            where = f"lower(block_range) <= {end} and (upper_inf(block_range) or upper(block_range) > {end})"
        elif "block$" in cols:
            where = f'"block$" <= {end}'
        else:
            where = "true"
        sel = ", ".join(f'"{c}"' for c in data)
        # Data sources have no id; their creation order (vid) matters.
        order = "id"
        if t == "data_sources$":
            order, where = "vid", f"lower(block_range) <= {end}"
        out = psql(f'select {sel} from "{schema}"."{t}" where {where} order by {order}', db, True)
        h.update(t.encode() + b"\0" + out.encode())
        rows[t] = out.count("\n")
    return h.hexdigest()[:16], rows


def run_once(args, run_no):
    case = CASES[args.case]
    db = "gn_" + args.case.replace("-", "_") + args.db_suffix
    fresh_db(db)
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    features = case.get("features", ["archive"])
    provider = f'{{ label = "rpc", url = "{case["rpc"]}", features = {json.dumps(features)} }}'
    datalake = ""
    if args.datalake:
        url = DATALAKES[args.case] if args.datalake == "auto" else args.datalake
        datalake = f'datalake = {{ url = "{url}" }}\n'
    cfg = work / f"{args.case}{args.db_suffix}.toml"
    cfg.write_text(f"""[store]
[store.primary]
connection = "postgresql://graph:graph@localhost:55433/{db}"
pool_size = 20

[chains]
ingestor = "default"

[chains.mainnet]
shard = "primary"
{datalake}provider = [ {provider} ]

[deployment]
[[deployment.rule]]
indexers = [ "default" ]
""")
    env = dict(os.environ, GRAPH_LOG=args.log_level, GRAPH_NODE_ID="default")
    for kv in args.env:
        k, v = kv.split("=", 1)
        env[k] = v
    b = args.port_base
    ports = dict(http=b, admin=b + 20, index=b + 30, metrics=b + 40)
    cmd = ["taskset", "-c", args.cpus, args.bin, "--config", str(cfg), "--node-id", "default",
           "--http-port", str(ports["http"]), "--admin-port", str(ports["admin"]),
           "--index-node-port", str(ports["index"]), "--metrics-port", str(ports["metrics"])]
    if args.ipfs != "builtin":
        cmd += ["--ipfs", args.ipfs]
    log = open(work / f"{args.case}{args.db_suffix}-{run_no}.log", "w")
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        api = f"http://127.0.0.1:{ports['admin']}" if args.ipfs == "builtin" else args.ipfs
        while True:
            try:
                http(f"http://127.0.0.1:{ports['admin']}", b"{}", {"content-type": "application/json"}, "POST", 1)
                break
            except urllib.error.HTTPError:
                break
            except Exception:
                if proc.poll() is not None:
                    raise RuntimeError(f"graph-node exited: see {log.name}")
                time.sleep(0.02)
        name = f"bench/{args.case}"
        deployment = upload(api, args.case)
        jsonrpc(ports["admin"], "subgraph_create", {"name": name})
        jsonrpc(ports["admin"], "subgraph_deploy", {"name": name, "ipfs_hash": deployment})
        t_deployed = time.monotonic()
        while True:
            st = None
            try:
                st = status(ports["index"], name)
            except Exception:
                pass
            if st:
                if st.get("fatalError"):
                    raise RuntimeError(f"subgraph failed: {st['fatalError']['message']}")
                chains = st.get("chains") or []
                latest = chains and chains[0].get("latestBlock")
                if latest and int(latest["number"]) >= case["end"]:
                    break
            if proc.poll() is not None:
                raise RuntimeError(f"graph-node exited: see {log.name}")
            if time.monotonic() - t0 > args.timeout:
                raise RuntimeError("timeout")
            time.sleep(0.05)
        t1 = time.monotonic()
        if args.metrics:
            (work / f"{args.case}{args.db_suffix}-{run_no}.metrics").write_bytes(
                http(f"http://127.0.0.1:{ports['metrics']}/metrics"))
    finally:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
    d, rows = digest(db, case["end"])
    return dict(case=args.case, seconds=round(t1 - t0, 2), deploy=round(t_deployed - t0, 2),
                after_deploy=round(t1 - t_deployed, 2), digest=d, rows=rows, deployment=deployment)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("case", choices=sorted(CASES))
    ap.add_argument("--bin", default=str(HERE.parent.parent / "target/release/graph-node"))
    ap.add_argument("--ipfs", default="builtin")
    ap.add_argument("--datalake", default=None, help="'auto' for the case's local datalake, or a URL")
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--timeout", type=float, default=7200)
    ap.add_argument("--log-level", default="info")
    ap.add_argument("--metrics", action="store_true", help="save the Prometheus metrics at the end")
    ap.add_argument("--port-base", type=int, default=18000)
    ap.add_argument("--cpus", default="0-7,10-31")
    ap.add_argument("--db-suffix", default="")
    ap.add_argument("--work", default=os.environ.get("TURBO_WORK", "/tmp/turbo-bench"))
    args = ap.parse_args()
    for i in range(args.runs):
        print(json.dumps(run_once(args, i)), flush=True)


if __name__ == "__main__":
    main()
