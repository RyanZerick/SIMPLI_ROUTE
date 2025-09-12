#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -------------------------------------------------------
# MODULO 1 : CONFIGURACION - CLI y entorno
# Objetivo: Cargar/actualizar vehículos desde Excel (RAIZ por defecto),
#           leer token desde .env (SIMPLIROUTE_TOKEN), evitar duplicados (UPSERT),
#           y verificar contra la "matriz" (API). Manejo de coordenadas (round 6).
# -------------------------------------------------------
import os, sys, json, argparse, re, datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import math
import pandas as pd
import requests

API_DEFAULT_BASE = "https://api.simpliroute.com"

def _ask(value: Optional[str], prompt: str, default: Optional[str] = None) -> str:
    if value is not None and str(value).strip():
        return str(value)
    s = f"{prompt}"
    if default is not None: s += f" [{default}]"
    s += ": "
    ans = input(s).strip()
    return ans or (default or "")

def _env_or(value: Optional[str], env_key: str) -> Optional[str]:
    return value or os.environ.get(env_key)

def load_env_from_root(script_dir: Path, explicit_env: Optional[str]=None) -> Optional[Path]:
    env_path = Path(explicit_env).expanduser().resolve() if explicit_env else (script_dir.parent / ".env").resolve()
    if not env_path.exists():
        return None
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=str(env_path), override=False)
    except Exception:
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k, v)
        except Exception:
            pass
    return env_path

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
    return default

def validate_df(df: pd.DataFrame) -> List[str]:
    errs: List[str] = []
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        errs.append(f"Faltan columnas obligatorias: {missing}")
        return errs
    for i, row in df.iterrows():
        if not str(row.get("unit_number") or "").strip():
            errs.append(f"Fila {i+1}: unit_number vacío")
        _ = hhmmss_or_default(row.get("shift_start"), "08:00:00")
        _ = hhmmss_or_default(row.get("shift_end"), "18:00:00")
        for cap in ("capacity1","capacity2","capacity3","cost"):
            v = row.get(cap)
            if pd.notna(v) and str(v).strip():
                try:
                    float(str(v).replace(",", "."))
                except Exception:
                    errs.append(f"Fila {i+1}: {cap} no es numérico: {v}")
        for col in ("start_lat","start_lon","end_lat","end_lon"):
            v = row.get(col)
            if pd.notna(v) and str(v).strip():
                try:
                    float(str(v).replace(",", "."))
                except Exception:
                    errs.append(f"Fila {i+1}: {col} no es numérico: {v}")
    return errs

# -------------------------------------------------------
# MODULO 3 : RUTAS/ARCHIVOS - resolver Excel desde RAIZ
# -------------------------------------------------------
DEFAULT_NAMES = ["vehiculos_DRIMER_cargamasiva.xlsx", "vehiculos_template.xlsx"]

def resolve_excel_path(cli_excel: Optional[str], script_dir: Path) -> str:
    if cli_excel and os.path.exists(cli_excel):
        return cli_excel
    for name in DEFAULT_NAMES:
        p = Path(name).expanduser().resolve()
        if p.exists():
            return str(p)
    root = script_dir.parent
    for name in DEFAULT_NAMES:
        p = (root / name).resolve()
        if p.exists():
            return str(p)
    raise SystemExit(f"No se encontró Excel. Probé: --excel, CWD({os.getcwd()}) y RAIZ({root}). Coloca el archivo en la raíz o pasa --excel.")

# -------------------------------------------------------
# MODULO 4 : COORDENADAS - sanitización
# Objetivo: Redondear a 6 decimales (max_digits=9 típico), validar rango y política.
# -------------------------------------------------------
def _round6(x: float) -> float:
    return float(f"{x:.6f}")

def sanitize_coord(v: Any, kind: str, policy: str) -> Optional[float]:
    # policy: keep | round | drop
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().replace(",", ".")
    if not s:
        return None
    try:
        val = float(s)
    except Exception:
        print(f"[WARN] {kind} malformado '{v}', se ignora.")
        return None

    # rango
    if "lat" in kind and not (-90 <= val <= 90):
        print(f"[WARN] {kind} fuera de rango ({val}), se ignora.")
        return None
    if "lon" in kind and not (-180 <= val <= 180):
        print(f"[WARN] {kind} fuera de rango ({val}), se ignora.")
        return None

    if policy == "drop":
        return None
    if policy == "round":
        val = _round6(val)

    # Validación de dígitos: max_digits=9 (p.ej. 2-3 enteros + 6 decimales)
    import re as _re
    canon = f"{abs(val):.6f}" if policy != "keep" else str(abs(val))
    digits = len(_re.sub(r"[^0-9]", "", canon))
    if digits > 9:
        if policy == "keep":
            val2 = _round6(val)
            canon2 = f"{abs(val2):.6f}"
            digits2 = len(_re.sub(r"[^0-9]", "", canon2))
            if digits2 <= 9:
                return val2
        print(f"[WARN] {kind} tiene {digits} dígitos (>9). Se descarta.")
        return None
    return val

# -------------------------------------------------------
# MODULO 5 : TRANSFORMACION - Parseo y payload vehículo
# -------------------------------------------------------
def parse_skills_field(v: Any) -> List[int]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return []
    skills: List[int] = []
    import re as _re
    for tok in _re.split(r"[,\s;]+", s):
        tok = tok.strip()
        if not tok or tok.lower() == "nan":
            continue
        try:
            skills.append(int(tok))
        except Exception:
            print(f"[WARN] skills_ids contiene valor no numérico '{tok}', se ignora.")
    return skills

def row_to_vehicle(row: pd.Series, users_by_username: Dict[str,int], coords_policy: str) -> Dict[str, Any]:
    name = (row.get("name") or row.get("unit_number") or "Vehículo").strip()
    payload: Dict[str, Any] = {
        "name": name,
        "license_plate": str(row.get("license_plate") or "").strip() or None,
        "capacity": float(str(row.get("capacity1")).replace(",", ".")) if pd.notna(row.get("capacity1")) and str(row.get("capacity1")).strip() else None,
        "capacity2": float(str(row.get("capacity2")).replace(",", ".")) if pd.notna(row.get("capacity2")) and str(row.get("capacity2")).strip() else None,
        "capacity3": float(str(row.get("capacity3")).replace(",", ".")) if pd.notna(row.get("capacity3")) and str(row.get("capacity3")).strip() else None,
        "shift_start": hhmmss_or_default(row.get("shift_start"), "08:00:00"),
        "shift_end": hhmmss_or_default(row.get("shift_end"), "18:00:00"),
        "location_start_address": str(row.get("start_address") or "").strip() or None,
        "location_end_address": str(row.get("end_address") or "").strip() or None,
        "reference_id": str(row.get("reference_id") or "").strip() or None,
        "cost": float(str(row.get("cost")).replace(",", ".")) if pd.notna(row.get("cost")) and str(row.get("cost")).strip() else None,
        "skills": parse_skills_field(row.get("skills_ids")),
        "codrivers": [],
    }
    # Coordenadas sanitizadas
    slat = sanitize_coord(row.get("start_lat"), "start_lat", coords_policy)
    slon = sanitize_coord(row.get("start_lon"), "start_lon", coords_policy)
    elat = sanitize_coord(row.get("end_lat"), "end_lat", coords_policy)
    elon = sanitize_coord(row.get("end_lon"), "end_lon", coords_policy)
    if slat is not None: payload["location_start_latitude"] = slat
    if slon is not None: payload["location_start_longitude"] = slon
    if elat is not None: payload["location_end_latitude"] = elat
    if elon is not None: payload["location_end_longitude"] = elon

    dd_user = str(row.get("default_driver_username") or "").strip()
    if dd_user and dd_user in users_by_username:
        payload["default_driver"] = users_by_username[dd_user]
    helpers = str(row.get("helpers_usernames") or "").strip()
    if helpers:
        for u in [x.strip() for x in helpers.split(",") if x.strip()]:
            if u in users_by_username:
                payload["codrivers"].append(users_by_username[u])

    clean = {k:v for k,v in payload.items() if not (v in (None, "", []) )}
    if "skills" in payload and len(payload["skills"])>0:
        clean["skills"] = payload["skills"]
    return clean

# -------------------------------------------------------
# MODULO 6 : API - helpers (HTTP y listados con paginación)
# -------------------------------------------------------
def http(session: requests.Session, method: str, path: str, json_body=None, params=None, timeout=30):
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
                path = next_url
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

def update_vehicle(session: requests.Session, vehicle_id: int, vpayload: Dict[str,Any]) -> Dict[str,Any]:
    status, payload = http(session, "PATCH", f"/v1/routes/vehicles/{vehicle_id}/", json_body=vpayload)
    if status >= 400:
        raise RuntimeError(f"Error actualizando vehículo {vehicle_id}: {status} {payload}")
    return payload

# -------------------------------------------------------
# MODULO 7 : CLAVES Y MAPAS - key normalizada (placa o unit_number)
# -------------------------------------------------------
def norm_key(s: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(s).upper()) if s is not None else ""

def build_vehicle_map(items: List[Dict[str,Any]], key_by: str) -> Dict[str, List[Dict[str,Any]]]:
    mp: Dict[str, List[Dict[str,Any]]] = {}
    for it in items:
        if key_by == "unit_number":
            key = norm_key(it.get("reference_id") or it.get("name") or it.get("license_plate"))
        else:
            key = norm_key(it.get("license_plate"))
        if not key:
            key = f"__no_key__:{it.get('id')}"
        mp.setdefault(key, []).append(it)
    return mp

# -------------------------------------------------------
# MODULO 8 : VERIFICACION - comparar Excel vs API
# -------------------------------------------------------
def verify_against_api(sess: requests.Session, df: pd.DataFrame, key_by: str) -> Dict[str, Any]:
    api_items = list_vehicles(sess)
    api_map = build_vehicle_map(api_items, key_by)
    excel_keys = set()
    for _, row in df.iterrows():
        key_val = (row.get("license_plate") if key_by=="license_plate" else row.get("unit_number"))
        k = norm_key(key_val)
        if k:
            excel_keys.add(k)
    api_keys = set(k for k in api_map.keys() if not k.startswith("__no_key__"))
    missing = sorted(excel_keys - api_keys)
    extra = sorted(api_keys - excel_keys)
    return {
        "excel_total": len(df),
        "api_total": len(api_items),
        "excel_keys": len(excel_keys),
        "api_keys": len(api_keys),
        "missing_keys": missing,
        "extra_keys": extra,
    }

# -------------------------------------------------------
# MODULO 9 : CLI - template/validate/upload/verify
# -------------------------------------------------------
def cmd_template(args):
    print("Plantilla esperada (columnas):")
    print(", ".join(ALL_COLS))
    print("Por defecto busca 'vehiculos_DRIMER_cargamasiva.xlsx' en RAIZ del proyecto.")

def cmd_validate(args):
    script_dir = Path(__file__).resolve().parent
    env_used = load_env_from_root(script_dir, args.env_file)
    if env_used: print(f"[ENV] cargado: {env_used}")
    token = _env_or(args.token, "SIMPLIROUTE_TOKEN")
    token = _ask(token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel, script_dir)
    print(f"[VALIDATE] Excel: {excel_path}")
    df = pd.read_excel(excel_path)
    errs = validate_df(df)
    for _, row in df.iterrows():
        _ = parse_skills_field(row.get("skills_ids"))
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs: print(" -", e)
        sys.exit(2)
    print("VALIDACION: OK")

def cmd_upload(args):
    script_dir = Path(__file__).resolve().parent
    env_used = load_env_from_root(script_dir, args.env_file)
    if env_used: print(f"[ENV] cargado: {env_used}")
    token = _env_or(args.token, "SIMPLIROUTE_TOKEN")
    token = _ask(token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel, script_dir)
    print(f"[UPLOAD] Excel: {excel_path}")
    sess = session(token, base)
    df = pd.read_excel(excel_path)
    errs = validate_df(df)
    if errs:
        print("VALIDACION: ERRORES")
        for e in errs: print(" -", e)
        sys.exit(2)

    key_by = args.key_by
    api_map = build_vehicle_map(list_vehicles(sess), key_by)
    users = list_users(sess)
    users_by_username = {str(u.get("username") or "").strip(): u.get("id") for u in users if u.get("id")}

    created = updated = skipped = 0
    for idx, row in df.iterrows():
        key_val = (row.get("license_plate") if key_by=="license_plate" else row.get("unit_number"))
        key = norm_key(key_val)
        if not key:
            print(f"[{idx+1}] SKIP: fila sin clave '{key_by}' usable.")
            skipped += 1
            continue

        vpayload = row_to_vehicle(row, users_by_username, args.coords_policy)
        if "name" not in vpayload:
            vpayload["name"] = str(row.get("unit_number") or f"Veh-{idx+1}")

        duplicates = api_map.get(key, [])
        if len(duplicates) == 0:
            if args.dry_run:
                print(f"[{idx+1}] CREATE (dry-run) -> {vpayload.get('name')} [{key}]")
                created += 1
            else:
                res = create_vehicle(sess, vpayload)
                print(f"[{idx+1}] CREATE -> ID={res.get('id')} :: {vpayload.get('name')} [{key}]")
                created += 1
        else:
            if args.mode == "skip-existing":
                print(f"[{idx+1}] SKIP (existe {len(duplicates)}) :: {vpayload.get('name')} [{key}]")
                skipped += 1
            else:
                if len(duplicates) > 1:
                    ids = [str(d.get('id')) for d in duplicates]
                    print(f"[WARN] Existen {len(duplicates)} vehículos con misma clave [{key}] -> IDs {', '.join(ids)}. Se actualizará el primero.")
                vid = duplicates[0].get("id")
                if args.dry_run:
                    print(f"[{idx+1}] UPDATE (dry-run) -> ID={vid} :: {vpayload.get('name')} [{key}]")
                    updated += 1
                else:
                    res = update_vehicle(sess, vid, vpayload)
                    print(f"[{idx+1}] UPDATE -> ID={res.get('id')} :: {vpayload.get('name')} [{key}]")
                    updated += 1

    print(f"RESUMEN: created={created}, updated={updated}, skipped={skipped} (mode={args.mode}, key_by={key_by}, dry_run={args.dry_run}, coords_policy={args.coords_policy})")

def cmd_verify(args):
    script_dir = Path(__file__).resolve().parent
    env_used = load_env_from_root(script_dir, args.env_file)
    if env_used: print(f"[ENV] cargado: {env_used}")
    token = _env_or(args.token, "SIMPLIROUTE_TOKEN")
    token = _ask(token, "Token SimpliRoute")
    base = _ask(args.base_url, "Base URL", API_DEFAULT_BASE)
    excel_path = resolve_excel_path(args.excel, script_dir)
    print(f"[VERIFY] Excel: {excel_path}")
    sess = session(token, base)
    df = pd.read_excel(excel_path)
    summary = verify_against_api(sess, df, args.key_by)
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_csv = Path(excel_path).with_name(f"verificacion_vehiculos_{ts}.csv")

    rows = [
        {"k":"excel_total","v":summary["excel_total"]},
        {"k":"api_total","v":summary["api_total"]},
        {"k":"excel_keys","v":summary["excel_keys"]},
        {"k":"api_keys","v":summary["api_keys"]},
        {"k":"missing_keys","v":";".join(summary["missing_keys"]) or "-"},
        {"k":"extra_keys","v":";".join(summary["extra_keys"]) or "-"}
    ]

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
    ap = argparse.ArgumentParser(description="Carga y verificación de vehículos SimpliRoute (RAIZ, UPSERT, .env, coords round)")
    ap.add_argument("--base-url", default=API_DEFAULT_BASE)
    ap.add_argument("--token", help="Si se omite, intentará SIMPLIROUTE_TOKEN desde .env/entorno")
    ap.add_argument("--excel", help="Ruta Excel; si se omite, busca en RAIZ del proyecto")
    ap.add_argument("--key-by", choices=["license_plate","unit_number"], default="license_plate", help="Clave para evitar duplicados (default: license_plate)")
    ap.add_argument("--mode", choices=["upsert","skip-existing"], default="upsert", help="Comportamiento si la clave ya existe (default: upsert)")
    ap.add_argument("--dry-run", action="store_true", help="No escribe en API, solo simula")
    ap.add_argument("--env-file", help="Ruta explícita del .env (si quieres forzar una distinta)")
    ap.add_argument("--coords-policy", choices=["round","keep","drop"], default="round", help="Qué hacer con lat/lon: round (6 dec), keep (tal cual), drop (ignorar)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("template", help="Mostrar columnas esperadas (plantilla)")
    p1.set_defaults(func=cmd_template)

    p2 = sub.add_parser("validate", help="Validar Excel")
    p2.set_defaults(func=cmd_validate)

    p3 = sub.add_parser("upload", help="Subir/actualizar vehículos en lote (UPSERT)")
    p3.set_defaults(func=cmd_upload)

    p4 = sub.add_parser("verify", help="Verificar cargados vs Excel (matriz API)")
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
