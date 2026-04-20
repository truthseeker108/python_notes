#!/usr/bin/env python3
"""
nxos_runner.py – Async parallel NX-OS command executor
Requires: asyncssh, pyyaml  (pip install asyncssh pyyaml)
Python 3.6+
"""

import asyncio
import asyncssh
import yaml
import logging
import sys
import csv
import json
import datetime
from pathlib import Path
from typing import List, Dict, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("nxos_runner.log"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config / input loaders
# ---------------------------------------------------------------------------

def load_hosts(hosts_file: str) -> List[str]:
    """
    Soronként egy hostname vagy IP.
    Üres sorok és # kommentek figyelmen kívül hagyva.
    """
    path = Path(hosts_file)
    if not path.exists():
        log.error(f"Hosts fájl nem található: {hosts_file}")
        sys.exit(1)

    hosts = []
    with path.open() as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                hosts.append(stripped)

    if not hosts:
        log.error("A hosts fájl üres vagy csak kommenteket tartalmaz.")
        sys.exit(1)

    log.info(f"Beolvasott eszközök: {len(hosts)}")
    return hosts


def load_commands(commands_file: str) -> List[str]:
    """
    Soronként egy NX-OS parancs.
    Üres sorok és # kommentek figyelmen kívül hagyva.
    """
    path = Path(commands_file)
    if not path.exists():
        log.error(f"Commands fájl nem található: {commands_file}")
        sys.exit(1)

    commands = []
    with path.open() as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                commands.append(stripped)

    if not commands:
        log.error("A commands fájl üres.")
        sys.exit(1)

    log.info(f"Beolvasott parancsok: {len(commands)}")
    return commands


def load_credentials(creds_file: str) -> Dict:
    """
    YAML struktúra:
      username: admin
      password: secret
      port: 22                  # opcionális, default 22
      timeout: 10               # opcionális, default 10s
      known_hosts: null         # null = disable host key checking (lab!)
    """
    path = Path(creds_file)
    if not path.exists():
        log.error(f"Credentials fájl nem található: {creds_file}")
        sys.exit(1)

    with path.open() as f:
        creds = yaml.safe_load(f)

    required = {"username", "password"}
    missing = required - set(creds.keys())
    if missing:
        log.error(f"Hiányzó kötelező mezők a credentials.yml-ből: {missing}")
        sys.exit(1)

    # Defaults
    creds.setdefault("port", 22)
    creds.setdefault("timeout", 10)
    creds.setdefault("known_hosts", None)

    return creds


# ---------------------------------------------------------------------------
# SSH worker
# ---------------------------------------------------------------------------

async def run_commands_on_host(
    host: str,
    commands: List[str],
    creds: Dict,
    semaphore: asyncio.Semaphore,
) -> Dict:
    """
    Egyetlen eszközre csatlakozik, lefuttatja az összes parancsot sorban,
    összegyűjti az outputot. A semaphore limitálja a párhuzamos sessionök számát.
    """
    result = {
        "host": host,
        "success": False,
        "outputs": {},
        "error": None,
    }

    async with semaphore:
        try:
            log.debug(f"[{host}] Kapcsolódás...")

            conn_kwargs = {
                "host": host,
                "port": creds["port"],
                "username": creds["username"],
                "password": creds["password"],
                "connect_timeout": creds["timeout"],
                # Ha known_hosts null → nem ellenőrzünk host key-t (csak lab env!)
                "known_hosts": creds["known_hosts"],
            }

            async with asyncssh.connect(**conn_kwargs) as conn:
                log.info(f"[{host}] Kapcsolódva")

                # NX-OS alapkövetelmény: terminal length 0 → no pager
                await conn.run("terminal length 0", check=False)

                for cmd in commands:
                    log.debug(f"[{host}] Futtatás: {cmd}")
                    res = await conn.run(cmd, check=False)
                    result["outputs"][cmd] = {
                        "stdout": res.stdout.strip(),
                        "stderr": res.stderr.strip(),
                        "exit_status": res.exit_status,
                    }

            result["success"] = True
            log.info(f"[{host}] OK – {len(commands)} parancs lefutott")

        except asyncssh.DisconnectError as e:
            result["error"] = f"SSH disconnect: {e}"
            log.warning(f"[{host}] Disconnect: {e}")
        except asyncssh.PermissionDenied:
            result["error"] = "Permission denied (helytelen credentials?)"
            log.warning(f"[{host}] Permission denied")
        except (OSError, asyncssh.Error) as e:
            result["error"] = str(e)
            log.warning(f"[{host}] SSH hiba: {e}")
        except asyncio.TimeoutError:
            result["error"] = f"Timeout ({creds['timeout']}s)"
            log.warning(f"[{host}] Timeout")

    return result


# ---------------------------------------------------------------------------
# Output / riport
# ---------------------------------------------------------------------------

def save_results(results: List[Dict], output_dir: str = "output"):
    """
    Minden eszköznek saját .txt fájl + egy összesített JSON + CSV summary.
    """
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    summary_rows = []

    for r in results:
        host = r["host"]
        safe_host = host.replace(".", "_").replace(":", "_")

        # Egyedi szöveges output
        txt_path = out / f"{safe_host}.txt"
        with txt_path.open("w") as f:
            f.write(f"Host: {host}\n")
            f.write(f"Success: {r['success']}\n")
            if r["error"]:
                f.write(f"Error: {r['error']}\n")
            f.write("=" * 60 + "\n")
            for cmd, data in r.get("outputs", {}).items():
                f.write(f"\n>>> {cmd}\n")
                f.write(data["stdout"] + "\n")
                if data["stderr"]:
                    f.write(f"[STDERR] {data['stderr']}\n")

        summary_rows.append({
            "host": host,
            "success": r["success"],
            "error": r["error"] or "",
            "commands_run": len(r.get("outputs", {})),
        })

    # JSON dump – teljes adat
    json_path = out / f"results_{timestamp}.json"
    with json_path.open("w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # CSV summary
    csv_path = out / f"summary_{timestamp}.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["host", "success", "error", "commands_run"])
        writer.writeheader()
        writer.writerows(summary_rows)

    log.info(f"Eredmények mentve: {out.resolve()}")
    log.info(f"  JSON: {json_path.name}")
    log.info(f"  CSV:  {csv_path.name}")
    log.info(f"  TXT:  {len(results)} egyedi fájl")


def print_summary(results: List[Dict]):
    ok = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print("\n" + "=" * 60)
    print(f"  ÖSSZESÍTŐ")
    print("=" * 60)
    print(f"  Összes eszköz : {len(results)}")
    print(f"  Sikeres       : {len(ok)}")
    print(f"  Sikertelen    : {len(failed)}")
    if failed:
        print("\n  Hibás eszközök:")
        for r in failed:
            print(f"    {r['host']:<30} {r['error']}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(
    hosts_file: str = "hosts",
    commands_file: str = "commands",
    creds_file: str = "credentials.yml",
    max_concurrent: int = 50,
    output_dir: str = "output",
):
    hosts = load_hosts(hosts_file)
    commands = load_commands(commands_file)
    creds = load_credentials(creds_file)

    log.info(f"Párhuzamos session limit: {max_concurrent}")

    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = [
        run_commands_on_host(host, commands, creds, semaphore)
        for host in hosts
    ]

    start = asyncio.get_event_loop().time()

    # return_exceptions=True → egy eszköz hibája nem töri el a többit
    results = await asyncio.gather(*tasks, return_exceptions=False)

    elapsed = asyncio.get_event_loop().time() - start
    log.info(f"Teljes futási idő: {elapsed:.1f}s")

    save_results(list(results), output_dir=output_dir)
    print_summary(list(results))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Async NX-OS parancs runner")
    parser.add_argument("--hosts",    default="hosts",           help="Hosts fájl (default: hosts)")
    parser.add_argument("--commands", default="commands",        help="Commands fájl (default: commands)")
    parser.add_argument("--creds",    default="credentials.yml", help="Credentials YAML (default: credentials.yml)")
    parser.add_argument("--workers",  type=int, default=50,      help="Max párhuzamos session (default: 50)")
    parser.add_argument("--output",   default="output",          help="Output könyvtár (default: output)")
    args = parser.parse_args()

    asyncio.run(
        main(
            hosts_file=args.hosts,
            commands_file=args.commands,
            creds_file=args.creds,
            max_concurrent=args.workers,
            output_dir=args.output,
        )
    )
