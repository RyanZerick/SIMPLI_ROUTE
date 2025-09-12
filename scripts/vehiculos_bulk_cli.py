#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------
# MODULO 1 : CONFIGURACION - CLI y entorno
# Objetivo: Leer Excel de vehículos desde la RAÍZ del proyecto (o ruta dada),
#           cargar por API y verificar contra la "matriz" de SimpliRoute.
# -------------------------------------------------------
import os, sys, json, argparse, re, datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
import requests

API_DEFAULT_BASE = "https://api.simpliroute.com"

def _ask(value: Optional[str], prompt: str, default: Optional[str] = None) -> str:
    if value is not None:
        return str(value)
    s = f"{prompt}"
    if default is not None: s += f" [{default}]"
    s += ": "
    ans = input(s).strip()
    return ans or (default or "")

def session(token: str, base_url: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {token}", "Content-Type": "application/json"})
    s.base_url = base_url.rstrip("/")
    return s

# -------------------------------------------------------
# MODULO 2 : VALIDACION - Plantilla y reglas
# Objetivo: Validar columnas, tiempos HH:MM:SS, y tipos numéricos básicos.
# -------------------------------------------------------
RE_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}$")

REQUIRED_COLS = ["unit_number"]
ALL_COLS = [
    "unit_number", "name", "license_plate",
    "capacity1", "capacity2", "capacity3",
    "shift_start", "shift_end",
    "start_address", "start_lat", "start_lon",
    "end_address", "end_lat", "end_lon",
    "default_driver_username", "helpers_usernames",
    "skills_ids", "reference_id", "cost"
]

def hhmmss_or_default(s: Any, default: str) -> str:
    if pd.isna(s) or not str(s).strip():
        return default
    t = str(s).strip()
    if RE_TIME.match(t):
        return t
    # Intentar parseo flexible; por simplicidad caer al default si no coincide regex
    return default

def validate_df(df: pd.DataFrame) -> List[str]:
    errs = []
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        errs.append(f"Faltan columnas obligatorias: {missing}")
        return errs
    for i, row in df.iterrows():
        if not str(row.get("unit_number") or "").strip():
            errs.append(f"Fila {i+1}: unit_number vacío")
        _ = hhmmss_or_default(row.get("shift_start"), "08:00:00")
        _ = hhmmss_or_default(row.get("shift_end"), "18:00:00")
        for cap in ("capacity1","capacity2","capacity3"):
            v = row.get(cap)
            if pd.notna(v) and str(v).strip():
                try:
                    float(v)
                except Exception:
                    errs.append(f"Fila {i+1}: {cap} no es numérico: {v}")
        for col in ("start_lat","start_lon","end_lat","end_lon"):
            v = row.get(col)
            if pd.notna(v) and str(v).strip():
                try:
                    float(v)
                except Exception:
                    errs.append(f"Fila {i+1}: {col} no es numérico: {v}")
    return errs

# -------------------------------------------------------
# MODULO 3 : RUTAS/ARCHIVOS - resolver Excel desde RAIZ
# Objetivo: Detectar Excel en: CLI -> CWD -> RAIZ del proyecto (../ del script).
# -------------------------------------------------------
DEFAULT_NAMES = ["vehiculos_DRIMER_cargamasiva.xlsx", "vehiculos_template.xlsx"]

def resolve_excel_path(cli_excel: Optional[str]) -> str:
    # 1) CLI directo
    if cli_excel and os.path.exists(cli_excel):
        return cli_excel
    # 2) CWD
    for name in DEFAULT_NAMES:
        p = Path(name).expanduser().resolve()
        if p.exists():
            return str(p)
    # 3) RAIZ: un nivel arriba del script (./scripts -> ..)
    script_dir = Path(__file__).resolve().parent
    root = script_dir.parent
    for name in DEFAULT_NAMES:
        p = (root / name).resolve()
        if p.exists():
            return str(p)
    raise SystemExit(f"No se encontró Excel. Probé: --excel, CWD({os.getcwd()}) y RAIZ({root}). Coloca el archivo en la raíz o pasa --excel.")

# -------------------------------------------------------
# MODULO 4 : TRANSFORMACION - Fila → payload vehículo
# Objetivo: Mapear Excel a JSON de /v1/routes/vehicles/
# -------------------------------------------------------
def row_to_vehicle(row: pd.Series, users_by_username: Dict[str,int]) -> Dict[str, Any]:
    name = (row.get("name") or row.get("unit_number") or "Vehículo").strip()
    payload = {
        "name": name,
        "license_plate": str(row.get("license_plate") or "").strip() or None,
        "capacity": float(row.get("capacity1")) if pd.notna(row.get("capacity1")) and str(row.get("capacity1")).strip() else None,
        "capacity2": float(row.get("capacity2")) if pd.notna(row.get("capacity2")) and str(row.get("capacity2")).strip() else None,
        "capacity3": float(row.get("capacity3")) if pd.notna(row.get("capacity3")) and str(row.get("capacity3")).strip() else None,
        "shift_start": hhmmss_or_default(row.get("shift_start"), "08:00:00"),
        "shift_end": hhmmss_or_default(row.get("shift_end"), "18:00:00"),
        "location_start_address": str(row.get("start_address") or "").strip() or None,
        "location_start_latitude": float(row.get("start_lat")) if pd.notna(row.get("start_lat")) and str(row.get("start_lat")).strip() else None,
        "location_start_longitude": float(row.get("start_lon")) if pd.notna(row.get("start_lon")) and str(row.get("start_lon")).strip() else None,
        "location_end_address": str(row.get("end_address") or "").strip() or None,
        "location_end_latitude": float(row.get("end_lat")) if pd.notna(row.get("end_lat")) and str(row.get("end_lat")).strip() else None,
        "location_end_longitude": float(row.get("end_lon")) if pd.notna(row.get("end_lon")) and str(row.get("end_lon")).strip() else None,
        "reference_id": str(row.get("reference_id") or "").strip() or None,
        "cost": float(row.get("cost")) if pd.notna(row.get("cost")) and str(row.get("cost")).strip() else None,
        "skills": [],
        "codrivers": [],
    }
    dd_user = str(row.get("default_driver_username") or "").strip()
    if dd_user and dd_user in users_by_username:
        payload["default_driver"] = users_by_username[dd_user]
    helpers = str(row.get("helpers_usernames") or "").strip()
    if helpers:
        for u in [x.strip() for x in helpers.split(",") if x.strip()]:
            if u in users_by_username:
                payload["codrivers"].append(users_by_username[u])
    skills_ids = str(row.get("skills_ids") or "").strip()
    if skills_ids:
        payload["skills"] = [int(x.strip()) for x in skills_ids.split(",") if x.strip()]
    clean = {k:v for k,v in payload.items() if v not in (None, "", [])}
    return clean

# -------------------------------------------------------
# MODULO 5 : API - helpers (HTTP y listados con paginación)
# Objetivo: GET/POST/DELETE y listar con manejo de 'results/next'.
# -------------------------------------------------------
def http(session: requests.Session, method: str, path: str, json_body=None, params=None, timeout=30):
    # Soporta path absoluto (next) o relativo
    url = path if str(path).startswith("http") else f"{session.base_url}{path}"
    r = session.request(method, url, json=json_body, params=params, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

def list_users(session: requests.Session) -> List[Dict[str,Any]]:
    status, payload = http(session, "GET", "/v1/accounts/drivers/")
    if status >= 400:
        raise RuntimeError(f"Error listando usuarios: {status} {payload}")
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "results" in payload:
        return payload["results"]
    return []

def list_vehicles(session: requests.Session) -> List[Dict[str,Any]]:
    items: List[Dict[str,Any]] = []
    path = "/v1/routes/vehicles/"
    while True:
        status, payload = http(session, "GET", path)
        if status >= 400:
            raise RuntimeError(f"Error listando vehículos: {status} {payload}")
        if isinstance(payload, list):
            items.extend(payload)
            break
        elif isinstance(payload, dict):
            items.extend(payload.get("results", []))
            next_url = payload.get("next")
            if next_url:
                path = next_url  # puede ser absoluto
                continue
            break
        else:
            break
    return items

def create_vehicle(session: requests.Session, vpayload: Dict[str,Any]) -> Dict[str,Any]:
    status, payload = http(session, "POST", "/v1/routes/vehicles/", json_body=vpayload)
    if status >= 400:
        raise RuntimeError(f"Error creando vehículo: {status} {payload}")
    return payload

# -------------------------------------------------------
# MODULO 6 : VERIFICACION - comparar Excel vs API
# Objetivo: Normalizar placas y reportar faltantes/sobrantes.
# -------------------------------------------------------
def norm_plate(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(s).upper())

def build_sets_from_excel(df: pd.DataFrame) -> Tuple[set, set]:
    plates = set()
    names  = set()
    for _, row in df.iterrows():
        if str(row.get("license_plate") or "").strip():
            plates.add(norm_plate(row["license_plate"]))
        if str(row.get("name") or "").strip():
            names.add(str(row["name"]).strip())
    return plates, names

def build_sets_from_api(items: List[Dict[str,Any]]) -> Tuple[set, set]:
    api_plates = set()
    api_names = set()
    for it in items:
        lp = it.get("license_plate")
        nm = it.get("name")
        if lp:
            api_plates.add(norm_plate(lp))
        if nm:
            api_names.add(str(nm).strip())
    return api_plates, api_names

def verify_against_api(sess: requests.Session, df: pd.DataFrame) -> Dict[str, Any]:
    api_items = list_vehicles(sess)
    excel_plates, excel_names = build_sets_from_excel(df)
    api_plates, api_names = build_sets_from_api(api_items)

    missing_by_plate = sorted(excel_plates - api_plates)
    extra_by_plate   = sorted(api_plates - excel_plates)

    return {
        "excel_total": len(df),
        "api_total": len(api_items),
        "excel_plates": len(excel_plates),
        "api_plates": len(api_plates),
        "missing_by_plate": missing_by_plate,
        "extra_by_plate": extra_by_plate,
    }

# -------------------------------------------------------
# MODULO 7 : CLI - template/validate/upload/verify
# Objetivo: Orquestar flujo y defaults de RAIZ.
# -------------------------------------------------------
def cmd_template(args):
    print("Plantilla esperada (columnas):")
    print(", ".join(ALL_COLS))
    print("Por defecto busca 'vehiculos_DRIMER_cargamasiva.xlsx' en RAIZ del proyecto.")

def cmd_validate(args):
    token = _ask(args.token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel)
    print(f"[VALIDATE] Excel: {excel_path}")
    df = pd.read_excel(excel_path)
    errs = validate_df(df)
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs: print(" -", e)
        sys.exit(2)
    print("VALIDACION: OK")

def cmd_upload(args):
    token = _ask(args.token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel)
    print(f"[UPLOAD] Excel: {excel_path}")
    sess = session(token, base)
    df = pd.read_excel(excel_path)
    errs = validate_df(df)
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs: print(" -", e)
        sys.exit(2)

    users = list_users(sess)
    users_by_username = {str(u.get("username") or "").strip(): u.get("id") for u in users if u.get("id")}
    created = []
    for idx, row in df.iterrows():
        vpayload = row_to_vehicle(row, users_by_username)
        if "name" not in vpayload:
            vpayload["name"] = str(row.get("unit_number") or f"Veh-{idx+1}")
        res = create_vehicle(sess, vpayload)
        created.append(res.get("id", res))
        print(f"[{idx+1}/{len(df)}] Creado vehículo ID={res.get('id')} -> {vpayload.get('name')}")
    print(f"TOTAL creados: {len(created)}")

def cmd_verify(args):
    token = _ask(args.token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel)
    print(f"[VERIFY] Excel: {excel_path}")
    sess = session(token, base)
    df = pd.read_excel(excel_path)
    summary = verify_against_api(sess, df)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_csv = Path(excel_path).with_name(f"verificacion_vehiculos_{ts}.csv")

    rows = []
    rows.append({"k":"excel_total","v":summary["excel_total"]})
    rows.append({"k":"api_total","v":summary["api_total"]})
    rows.append({"k":"excel_plates","v":summary["excel_plates"]})
    rows.append({"k":"api_plates","v":summary["api_plates"]})
    rows.append({"k":"missing_by_plate","v":";".join(summary["missing_by_plate"]) or "-"})
    rows.append({"k":"extra_by_plate","v":";".join(summary["extra_by_plate"]) or "-"})

    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")
    print("---- RESUMEN ----")
    for r in rows:
        print(f"{r['k']}: {r['v']}")
    print(f"Reporte: {out_csv}")

def cmd_upload_and_verify(args):
    cmd_upload(args)
    print("\n[POST] Verificando carga contra API...\n")
    cmd_verify(args)

def build_parser():
    ap = argparse.ArgumentParser(description="Carga y verificación de vehículos SimpliRoute (busca Excel en RAÍZ por defecto)")
    ap.add_argument("--base-url", default=API_DEFAULT_BASE)
    ap.add_argument("--token")
    ap.add_argument("--excel", help="Ruta Excel; si se omite, busca en RAIZ del proyecto")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("template", help="Mostrar columnas esperadas (plantilla)")
    p1.set_defaults(func=cmd_template)

    p2 = sub.add_parser("validate", help="Validar Excel")
    p2.set_defaults(func=cmd_validate)

    p3 = sub.add_parser("upload", help="Subir vehículos en lote (Excel en RAIZ por defecto)")
    p3.set_defaults(func=cmd_upload)

    p4 = sub.add_parser("verify", help="Verificar vehículos cargados vs Excel (matriz API)")
    p4.set_defaults(func=cmd_verify)

    p5 = sub.add_parser("upload-and-verify", help="Subir y verificar en un paso")
    p5.set_defaults(func=cmd_upload_and_verify)

    return ap

def main():
    ap = build_parser()
    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
