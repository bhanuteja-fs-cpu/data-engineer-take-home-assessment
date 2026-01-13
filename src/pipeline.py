import argparse
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml


# ----- simple transforms (requirements) -----
def title(x): return str(x).strip().title() if pd.notna(x) else None
def lower(x): return str(x).strip().lower() if pd.notna(x) else None

def iso_date(x):
    if pd.isna(x): return None
    s = str(x).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return None

def phone_fmt(x):
    if pd.isna(x): return None
    d = re.sub(r"\D", "", str(x))
    if len(d) == 11 and d.startswith("1"): d = d[1:]
    return f"{d[:3]}-{d[3:6]}-{d[6:]}" if len(d) == 10 else None


# ----- config + ingestion -----
STANDARD = ["external_id", "first_name", "last_name", "dob", "email", "phone"]

def load_partners(config_path: Path) -> dict:
    return yaml.safe_load(config_path.read_text(encoding="utf-8"))["partners"]

def read_partner(input_dir: Path, cfg: dict) -> pd.DataFrame:
    df = pd.read_csv(input_dir / cfg["file_name"], sep=cfg["delimiter"], dtype=str, engine="python")

    # cfg["mappings"] is: standard_field -> partner_column
    rename_map = {partner_col: std for std, partner_col in cfg["mappings"].items()}
    df = df.rename(columns=rename_map)[STANDARD]

    df["first_name"] = df["first_name"].apply(title)
    df["last_name"]  = df["last_name"].apply(title)
    df["dob"]        = df["dob"].apply(iso_date)
    df["email"]      = df["email"].apply(lower)
    df["phone"]      = df["phone"].apply(phone_fmt)
    df["partner_code"] = cfg["partner_code"]

    return df

def main():
    ap = argparse.ArgumentParser(description="Healthcare Eligibility Pipeline (config-driven)")
    ap.add_argument("--config", required=True)
    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--drop_missing_external_id", action="store_true")  # bonus
    args = ap.parse_args()

    partners = load_partners(Path(args.config))
    out = pd.concat([read_partner(Path(args.input_dir), cfg) for cfg in partners.values()], ignore_index=True)

    if args.drop_missing_external_id:
        out = out[out["external_id"].notna() & (out["external_id"].str.strip() != "")]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Done ✅ {out_path} (rows={len(out)})")

if __name__ == "__main__":
    main()