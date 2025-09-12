#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------
# MODULO 1 : CONFIGURACION - CLI y entorno
# Objetivo: Leer Excel de vehículos y cargar por API; soportar 'template', 'validate' y 'upload'.
# -------------------------------------------------------
import os, sys, json, argparse, re
from typing import Any, Dict, List, Optional
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
    # Intentar parsear formato H:M o números; pero por simplicidad, caer al default
    return default

def validate_df(df: pd.DataFrame) -> List[str]:
    errs = []
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        errs.append(f"Faltan columnas obligatorias: {missing}")
        return errs
    # Validar tiempos y numéricos
    for i, row in df.iterrows():
        # unit_number
        if not str(row.get("unit_number") or "").strip():
            errs.append(f"Fila {i+1}: unit_number vacío")
        # times
        _ = hhmmss_or_default(row.get("shift_start"), "08:00:00")
        _ = hhmmss_or_default(row.get("shift_end"), "18:00:00")
        # capacities
        for cap in ("capacity1","capacity2","capacity3"):
            v = row.get(cap)
            if pd.notna(v) and str(v).strip():
                try:
                    float(v)
                except Exception:
                    errs.append(f"Fila {i+1}: {cap} no es numérico: {v}")
        # lat/lon
        for col in ("start_lat","start_lon","end_lat","end_lon"):
            v = row.get(col)
            if pd.notna(v) and str(v).strip():
                try:
                    float(v)
                except Exception:
                    errs.append(f"Fila {i+1}: {col} no es numérico: {v}")
    return errs

# -------------------------------------------------------
# MODULO 3 : TRANSFORMACION - Fila → payload vehículo
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
    # default_driver by username
    dd_user = str(row.get("default_driver_username") or "").strip()
    if dd_user and dd_user in users_by_username:
        payload["default_driver"] = users_by_username[dd_user]
    # helpers codrivers (comma-separated usernames)
    helpers = str(row.get("helpers_usernames") or "").strip()
    if helpers:
        for u in [x.strip() for x in helpers.split(",") if x.strip()]:
            if u in users_by_username:
                payload["codrivers"].append(users_by_username[u])
    # skills ids
    skills_ids = str(row.get("skills_ids") or "").strip()
    if skills_ids:
        payload["skills"] = [int(x.strip()) for x in skills_ids.split(",") if x.strip()]
    # limpiar None -> quitar claves
    clean = {k:v for k,v in payload.items() if v not in (None, "", [])}
    return clean

# -------------------------------------------------------
# MODULO 4 : API - Usuarios/vehículos
# Objetivo: Resolver IDs de usuarios por username y crear vehículos.
# -------------------------------------------------------
def http(session: requests.Session, method: str, path: str, json_body=None, params=None, timeout=30):
    url = f"{session.base_url}{path}"
    r = session.request(method, url, json=json_body, params=params, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text

def list_users(session: requests.Session) -> List[Dict[str,Any]]:
    # Intento simple sin paginación
    status, payload = http(session, "GET", "/v1/accounts/drivers/")
    if status >= 400:
        raise RuntimeError(f"Error listando usuarios: {status} {payload}")
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "results" in payload:
        return payload["results"]
    return []

def create_vehicle(session: requests.Session, vpayload: Dict[str,Any]) -> Dict[str,Any]:
    status, payload = http(session, "POST", "/v1/routes/vehicles/", json_body=vpayload)
    if status >= 400:
        raise RuntimeError(f"Error creando vehículo: {status} {payload}")
    return payload

# -------------------------------------------------------
# MODULO 5 : CLI - template/validate/upload
# Objetivo: Orquestar el flujo de creación en lote.
# -------------------------------------------------------
def cmd_template(args):
    print("Plantilla esperada (columnas):")
    print(", ".join(ALL_COLS))
    print("Completa y usa 'validate' y 'upload'.")

def cmd_validate(args):
    token = _ask(args.token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel = _ask(args.excel, "Ruta Excel")
    df = pd.read_excel(excel)
    errs = validate_df(df)
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs:
            print(" -", e)
        sys.exit(2)
    else:
        print("VALIDACION: OK")

def cmd_upload(args):
    token = _ask(args.token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel = _ask(args.excel, "Ruta Excel")
    sess = session(token, base)
    df = pd.read_excel(excel)
    errs = validate_df(df)
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs: print(" -", e)
        sys.exit(2)

    # Construir mapa username -> user_id
    users = list_users(sess)
    users_by_username = {}
    for u in users:
        uname = str(u.get("username") or "").strip()
        uid = u.get("id")
        if uname and uid:
            users_by_username[uname] = uid

    created = []
    for idx, row in df.iterrows():
        vpayload = row_to_vehicle(row, users_by_username)
        if "name" not in vpayload:
            vpayload["name"] = str(row.get("unit_number") or f"Veh-{idx+1}")
        res = create_vehicle(sess, vpayload)
        created.append(res.get("id", res))
        print(f"[{idx+1}/{len(df)}] Creado vehículo ID={res.get('id')} -> {vpayload.get('name')}")
    print(f"TOTAL creados: {len(created)}")

def main():
    ap = argparse.ArgumentParser(description="Carga masiva de vehículos SimpliRoute")
    ap.add_argument("--base-url", default=API_DEFAULT_BASE)
    ap.add_argument("--token")
    ap.add_argument("--excel")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("template", help="Mostrar columnas esperadas (plantilla)")
    p1.set_defaults(func=cmd_template)
    p2 = sub.add_parser("validate", help="Validar Excel")
    p2.set_defaults(func=cmd_validate)
    p3 = sub.add_parser("upload", help="Subir vehículos en lote")
    p3.set_defaults(func=cmd_upload)
    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
