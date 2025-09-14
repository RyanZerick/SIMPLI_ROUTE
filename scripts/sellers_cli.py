#!/usr/bin/env python3
# -------------------------------------------------------
# MODULO 1 : CLI Sellers (SimpliRoute)
# Objetivo: sincronizar sellers desde Excel (config_) con la API:
# - Completar seller_uuid (lookup).
# - (Opcional) crear sellers si el tenant lo permite.
# -------------------------------------------------------

import argparse, os, sys, json
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
import requests
import pandas as pd
from pathlib import Path

# -------------------------------------------------------
# MODULO 2 : Rutas y configuración
# - Detectar raíz del proyecto (carpeta padre de /scripts)
# - Cargar .env desde raíz
# - Sesión HTTP y utilitarios
# -------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]  # .../<raiz>
SCRIPTS_DIR = ROOT_DIR / "scripts"

DEFAULT_CONFIG_XLSX = ROOT_DIR / "config_sellers.xlsx"
DEFAULT_TEMPLATE_XLSX = ROOT_DIR / "template_sellers.xlsx"

@dataclass
class SRConfig:
    base_url: str
    token: str
    excel_path: Path
    timeout: int = 30
    enable_create: bool = False

# (1) load_env(): .env de raíz (fallback CWD)
def load_env() -> Dict[str, str]:
    for candidate in (ROOT_DIR / ".env", Path.cwd() / ".env"):
        if candidate.exists():
            try:
                with open(candidate, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            os.environ.setdefault(k.strip(), v.strip())
                print(f"[ENV] cargado: {candidate}")
                break
            except Exception as e:
                print(f"[ENV] advertencia: no se pudo leer {candidate} ({e})")
    return dict(os.environ)

# (2) ensure_base_url(): normalizar /v1/
def ensure_base_url(url: str) -> str:
    url = url.rstrip("/")
    if url.endswith("/v1"):
        return url + "/"
    if url.endswith("/v1/"):
        return url
    return url + "/v1/"

# (3) make_session(): requests con Token
def make_session(cfg: SRConfig) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Token {cfg.token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return s

# (4) path_resolver(): preferir raíz; soportar override por --excel
def path_resolver(path_opt: Optional[str], default_in_root: Path) -> Path:
    if path_opt:
        p = Path(path_opt).expanduser().resolve()
        return p
    # prefer root default; si no existe, probar en CWD
    if default_in_root.exists():
        return default_in_root
    alt = Path.cwd() / default_in_root.name
    return alt.resolve()

# -------------------------------------------------------
# MODULO 3 : I/O de Excel
# - Leer config_...xlsx
# - Validar columnas
# - Escribir seller_uuid
# - Reporte de faltantes
# -------------------------------------------------------

REQUIRED_COLUMNS = [
    "seller_code","seller_name","description","wa_policy",
    "notify_emails_csv","notify_whatsapp_csv",
    "allowed_fulfillment_csv","active","seller_uuid"
]

def read_excel(excel_path: Path) -> pd.DataFrame:
    if not excel_path.exists():
        raise FileNotFoundError(f"No se encontró Excel: {excel_path}")
    df = pd.read_excel(excel_path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en Excel: {missing}")
    df["seller_code"] = df["seller_code"].astype(str).str.strip()
    df["seller_name"] = df["seller_name"].astype(str).str.strip()
    df["active"] = df["active"].apply(lambda x: str(x).strip().upper() in ("TRUE","VERDADERO","1","SI","YES"))
    df["seller_uuid"] = df.get("seller_uuid", "").astype(str).str.strip()
    return df

def write_excel_with_uuids(excel_path: Path, df: pd.DataFrame) -> None:
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as wr:
        df.to_excel(wr, index=False, sheet_name="sellers")
    print(f"[WRITE] actualizado: {excel_path}")

def export_missing_report(base_path: Path, missing_rows: List[Dict[str,Any]]) -> Optional[Path]:
    if not missing_rows:
        return None
    out = base_path.parent / "sellers_missing_for_creation.csv"
    pd.DataFrame(missing_rows).to_csv(out, index=False, encoding="utf-8")
    print(f"[REPORT] faltantes -> {out}")
    return out

# -------------------------------------------------------
# MODULO 4 : API Sellers
# - GET /v1/sellers/
# - (Opcional) POST /v1/sellers/ si tenant lo permite
# -------------------------------------------------------

def api_list_sellers(sess: requests.Session, cfg: SRConfig) -> List[Dict[str,Any]]:
    url = ensure_base_url(cfg.base_url) + "sellers/"
    r = sess.get(url, timeout=cfg.timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Error listando sellers: {r.status_code} {r.text}")
    return r.json()

def api_create_seller(sess: requests.Session, cfg: SRConfig, name: str, alias: str, language: str="es") -> Dict[str,Any]:
    if not cfg.enable_create:
        raise RuntimeError("Creación de sellers deshabilitada. Esta operación depende de features habilitadas por CS.")
    url = ensure_base_url(cfg.base_url) + "sellers/"
    payload = {"name": name, "alias": alias, "language": language}
    r = sess.post(url, data=json.dumps(payload), timeout=cfg.timeout)
    if r.status_code not in (200,201):
        raise RuntimeError(f"Error creando seller '{name}': {r.status_code} {r.text}")
    return r.json()

def match_existing(sellers_api: List[Dict[str,Any]]) -> Tuple[Dict[str,Any], Dict[str,Any]]:
    by_name, by_alias = {}, {}
    for s in sellers_api:
        if s.get("name"):
            by_name[s["name"].strip().lower()] = s
        if s.get("alias"):
            by_alias[s["alias"].strip().lower()] = s
    return by_name, by_alias

# -------------------------------------------------------
# MODULO 5 : Comandos CLI
# - template: crear template_sellers.xlsx en raíz
# - validate: validar estructura del config
# - sync: completar seller_uuid y reportar faltantes
# - create-missing: (opcional) crear sellers
# -------------------------------------------------------

TEMPLATE_MIN = [{
    "seller_code":"SAGA",
    "seller_name":"Saga Falabella",
    "description":"Retail aliado",
    "wa_policy":"DOMICILIO_ONLY",
    "notify_emails_csv":"",
    "notify_whatsapp_csv":"",
    "allowed_fulfillment_csv":"DOMICILIO,CD,EXHIBICION,RECOJO_TIENDA,AGENCIA",
    "active": True,
    "seller_uuid":""
}]

def cmd_template(args):
    path = path_resolver(args.excel, DEFAULT_TEMPLATE_XLSX)
    if path.exists():
        print(f"[TEMPLATE] ya existe: {path}")
        return
    df = pd.DataFrame(TEMPLATE_MIN, columns=REQUIRED_COLUMNS)
    with pd.ExcelWriter(path, engine="xlsxwriter") as wr:
        df.to_excel(wr, index=False, sheet_name="sellers")
    print(f"[TEMPLATE] creado: {path}")

def build_cfg_from_args(args) -> SRConfig:
    load_env()
    token = args.token or os.environ.get("SIMPLIROUTE_TOKEN") or ""
    if not token:
        token = input(">> Ingresa tu Token de SimpliRoute: ").strip()
    base_url = args.base_url or os.environ.get("SIMPLIROUTE_BASE_URL","https://api.simpliroute.com")
    excel_path = path_resolver(args.excel, DEFAULT_CONFIG_XLSX)
    print(f"[CFG] Excel: {excel_path}")
    return SRConfig(base_url=base_url, token=token, excel_path=excel_path, enable_create=args.enable_create)

def cmd_validate(args):
    cfg = build_cfg_from_args(args)
    df = read_excel(cfg.excel_path)
    print(f"[VALIDATE] filas={len(df)} columnas_ok={len(REQUIRED_COLUMNS)} -> {cfg.excel_path}")

def cmd_sync(args):
    cfg = build_cfg_from_args(args)
    sess = make_session(cfg)
    # ping auth
    me = sess.get(ensure_base_url(cfg.base_url) + "users/me/", timeout=cfg.timeout)
    print(f"[AUTH] status={me.status_code}")
    df = read_excel(cfg.excel_path)

    sellers_api = api_list_sellers(sess, cfg)
    by_name, by_alias = match_existing(sellers_api)

    missing, updated = [], 0
    for i, row in df.iterrows():
        if str(row.get("seller_uuid","")).strip():
            continue
        name = row["seller_name"].strip()
        code = row["seller_code"].strip()
        hit = by_name.get(name.lower()) or by_alias.get(code.lower())
        if hit:
            df.at[i, "seller_uuid"] = hit.get("id","")
            updated += 1
        else:
            missing.append({
                "seller_code": row["seller_code"],
                "seller_name": row["seller_name"],
                "description": row["description"]
            })

    write_excel_with_uuids(cfg.excel_path, df)
    export_missing_report(cfg.excel_path, missing)
    print(f"[SYNC] actualizados={updated} faltantes={len(missing)}")

def cmd_create_missing(args):
    cfg = build_cfg_from_args(args)
    if not cfg.enable_create:
        print("[CREATE] Deshabilitado. Usa --enable-create si tu cuenta lo soporta.")
        return
    sess = make_session(cfg)
    df = read_excel(cfg.excel_path)

    sellers_api = api_list_sellers(sess, cfg)
    by_name, by_alias = match_existing(sellers_api)

    created = 0
    for i, row in df.iterrows():
        if str(row.get("seller_uuid","")).strip():
            continue
        name = row["seller_name"].strip()
        code = row["seller_code"].strip()
        hit = by_name.get(name.lower()) or by_alias.get(code.lower())
        if hit:
            df.at[i, "seller_uuid"] = hit.get("id","")
            continue
        obj = api_create_seller(sess, cfg, name=name, alias=code, language="es")
        df.at[i, "seller_uuid"] = obj.get("id","")
        created += 1

    write_excel_with_uuids(cfg.excel_path, df)
    print(f"[CREATE] creados={created}")

# -------------------------------------------------------
# MODULO 6 : Parser CLI
# - Subcomandos y parámetros (pensado para futura GUI)
# -------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CLI de Sellers (SimpliRoute)")
    p.add_argument("--base-url", help="Base URL de la API (default: https://api.simpliroute.com)", default=None)
    p.add_argument("--token", help="Token SR (si no, usa SIMPLIROUTE_TOKEN del .env en raíz)", default=None)
    p.add_argument("--excel", help="Ruta Excel (default: raiz/config_sellers.xlsx)", default=None)
    p.add_argument("--enable-create", action="store_true", help="Habilita POST /v1/sellers/ si tu tenant lo soporta")

    sp = p.add_subparsers(dest="cmd", required=True)

    sp_t = sp.add_parser("template", help="Generar template en raiz/template_sellers.xlsx")
    sp_t.set_defaults(func=cmd_template)

    sp_v = sp.add_parser("validate", help="Validar estructura del config")
    sp_v.set_defaults(func=cmd_validate)

    sp_s = sp.add_parser("sync", help="Completar seller_uuid desde API y reportar faltantes")
    sp_s.set_defaults(func=cmd_sync)

    sp_c = sp.add_parser("create-missing", help="(Opcional) Crear faltantes si POST está habilitado")
    sp_c.set_defaults(func=cmd_create_missing)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
