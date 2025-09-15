#!/usr/bin/env python3
# -------------------------------------------------------
# MODULO 1 : CLI Usuarios (Web & Móvil) para SimpliRoute
# Objetivo - funciones:
#  - Leer configuraciones desde Excel en la RAÍZ (config_web_users.xlsx, config_mobile_users.xlsx)
#  - Validar estructura y datos antes de enviar
#  - Sincronizar (crear/actualizar) usuarios vía API
#  - Probar conectividad creando un usuario de prueba
# -------------------------------------------------------
# Observaciones
# 1) API documentada para "drivers": /v1/accounts/drivers/ (móvil). Web roles vía API pueden depender de habilitaciones del tenant.
# 2) Si no existe permiso para crear web users por API, se reportará y se omitirán (archivo CSV de pendientes).
# 3) Token se toma de .env (SIMPLIROUTE_TOKEN) o --token. Base URL: .env SIMPLIROUTE_BASE_URL o --base-url.
# 4) Futuros complementos GUI: este CLI ya está modularizado y parámetros son explícitos.
# -------------------------------------------------------

# -------------------------------------------------------
# MODULO 2 : Imports y utilitarios base
# - argparse/requests/pandas/Path
# - carga de .env desde raíz o CWD
# - normalización de base_url
# -------------------------------------------------------

import argparse, os, sys, json, time, random, string
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

import requests
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]  # .../<raiz>
SCRIPTS_DIR = ROOT_DIR / "scripts"

DEFAULT_WEB_XLSX    = ROOT_DIR / "config_web_users.xlsx"
DEFAULT_MOBILE_XLSX = ROOT_DIR / "config_mobile_users.xlsx"
SELLERS_XLSX        = ROOT_DIR / "config_sellers.xlsx"

@dataclass
class SRConfig:
    base_url: str
    token: str
    web_path: Path
    mobile_path: Path
    timeout: int = 30
    try_web_api: bool = True   # intentar crear web users por API; si falla, se reporta

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
                            key = k.strip()
                            val = v.strip()
                            # elimina comillas envolventes si existen
                            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                                val = val[1:-1]
                            os.environ[key] = val  # sobreescribe para evitar valores “sucios”
                print(f"[ENV] cargado: {candidate}")
                break
            except Exception as e:
                print(f"[ENV] advertencia: no se pudo leer {candidate} ({e})")
    return dict(os.environ)


def ensure_base_url(url: str) -> str:
    url = (url or "https://api.simpliroute.com").rstrip("/")
    if url.endswith("/v1"):
        return url + "/"
    if url.endswith("/v1/"):
        return url
    return url + "/v1/"

def path_resolver(path_opt: Optional[str], default_path: Path) -> Path:
    if path_opt:
        return Path(path_opt).expanduser().resolve()
    if default_path.exists():
        return default_path
    alt = Path.cwd() / default_path.name
    return alt.resolve()

def make_session(cfg: SRConfig) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Token {cfg.token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return s

# -------------------------------------------------------
# MODULO 3 : API - autenticación y endpoints
# - Ping auth
# - Drivers (listar/crear/editar/borrar)
# - Helpers con fallback array<->dict
# -------------------------------------------------------

def ping_auth(sess: requests.Session, cfg: SRConfig) -> int:
    base = ensure_base_url(cfg.base_url)
    for ep in ("accounts/me/",):
        try:
            r = sess.get(base + ep, timeout=cfg.timeout)
            print(f"[AUTH] GET {ep} -> {r.status_code}")
            if r.status_code == 200:
                return 200
        except requests.RequestException as e:
            print(f"[AUTH] error de red: {e}")
            return 0
    return r.status_code

def api_list_drivers(sess: requests.Session, cfg: SRConfig) -> List[Dict[str, Any]]:
    base = ensure_base_url(cfg.base_url)
    url = base + "accounts/drivers/"
    try:
        r = sess.get(url, timeout=cfg.timeout)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and "results" in data:
                return data.get("results", [])
            if isinstance(data, list):
                return data
            return []
        raise RuntimeError(f"List drivers -> {r.status_code} {r.text[:200]}")
    except requests.RequestException as e:
        raise RuntimeError(f"List drivers error red: {e}")

def _post_list(sess: requests.Session, url: str, rows: List[Dict[str, Any]], timeout: int):
    return sess.post(url, data=json.dumps(rows), timeout=timeout)

def _post_dict(sess: requests.Session, url: str, row: Dict[str, Any], timeout: int):
    return sess.post(url, data=json.dumps(row), timeout=timeout)

def _put_list(sess: requests.Session, url: str, rows: List[Dict[str, Any]], timeout: int):
    return sess.put(url, data=json.dumps(rows), timeout=timeout)

def _put_dict(sess: requests.Session, url: str, row: Dict[str, Any], timeout: int):
    return sess.put(url, data=json.dumps(row), timeout=timeout)

# PATCH helpers (igual que _put_list/_put_dict)
def _patch_list(sess: requests.Session, url: str, rows: List[Dict[str, Any]], timeout: int):
    return sess.patch(url, data=json.dumps(rows), timeout=timeout)

def _patch_dict(sess: requests.Session, url: str, row: Dict[str, Any], timeout: int):
    return sess.patch(url, data=json.dumps(row), timeout=timeout)

def _json_or_text(r: requests.Response):
    try:
        return r.json()
    except Exception:
        return r.text


def api_create_drivers(sess: requests.Session, cfg: SRConfig, rows: List[Dict[str, Any]]) -> Tuple[int, Any, str]:
    """Crea drivers esperando LISTA como body (según doc)."""
    base = ensure_base_url(cfg.base_url)
    url = base + "accounts/drivers/"
    try:
        r = _post_list(sess, url, rows, cfg.timeout)
        payload = _json_or_text(r)
        return (r.status_code, payload, r.text)
    except requests.RequestException as e:
        raise RuntimeError(f"Create drivers error red: {e}")

def api_create_driver_fallback(sess: requests.Session, cfg: SRConfig, row: Dict[str, Any]) -> Tuple[int, Any, str]:
    """
    Intenta crear driver con fallback y tolerancia a timeouts:
    1) POST LISTA [row]
       - si 400 con 'Expected a dictionary' -> POST DICT {row}
    2) Si hay ReadTimeout en cualquiera, reintenta UNA vez con timeout aumentado (min(120, cfg.timeout*2)).
    """
    base = ensure_base_url(cfg.base_url)
    url = base + "accounts/drivers/"

    def _do_list(to):
        return _post_list(sess, url, [row], to)

    def _do_dict(to):
        return _post_dict(sess, url, row, to)

    # intento 1: LISTA
    try:
        r = _do_list(cfg.timeout)
        payload = _json_or_text(r)
        # si el backend quiere DICT
        if r.status_code == 400 and isinstance(payload, dict) and any("Expected a dictionary" in str(v) for v in payload.values()):
            # intento 2: DICT
            try:
                r2 = _do_dict(cfg.timeout)
                payload2 = _json_or_text(r2)
                return (r2.status_code, payload2, r2.text)
            except requests.exceptions.ReadTimeout:
                # reintento DICT con más timeout
                to2 = min(120, cfg.timeout * 2)
                r3 = _do_dict(to2)
                payload3 = _json_or_text(r3)
                return (r3.status_code, payload3, r3.text)
        return (r.status_code, payload, r.text)

    except requests.exceptions.ReadTimeout:
        # Reintento LISTA con más timeout
        to2 = min(120, cfg.timeout * 2)
        try:
            r4 = _do_list(to2)
            payload4 = _json_or_text(r4)
            # si ahora dice 'Expected a dictionary', probamos DICT con timeout largo
            if r4.status_code == 400 and isinstance(payload4, dict) and any("Expected a dictionary" in str(v) for v in payload4.values()):
                r5 = _do_dict(to2)
                payload5 = _json_or_text(r5)
                return (r5.status_code, payload5, r5.text)
            return (r4.status_code, payload4, r4.text)
        except requests.exceptions.ReadTimeout as e2:
            return (0, {}, f"ReadTimeout tras reintento: {e2}")
    except requests.RequestException as e:
        raise RuntimeError(f"Create driver (fallback) error red: {e}")


def api_update_driver(sess: requests.Session, cfg: SRConfig, user_id: int, row: Dict[str, Any]) -> Tuple[int, Any, str]:
    """PUT con fallback: intenta LISTA -> si ve 'Expected a dictionary', reintenta OBJETO."""
    base = ensure_base_url(cfg.base_url)
    url = base + f"accounts/drivers/{user_id}/"
    try:
        r = _put_list(sess, url, [row], cfg.timeout)
        payload = _json_or_text(r)
        if r.status_code == 400 and isinstance(payload, (dict,)) and any(
            "Expected a dictionary" in str(v) for v in payload.values()
        ):
            r2 = _put_dict(sess, url, row, cfg.timeout)
            payload2 = _json_or_text(r2)
            return (r2.status_code, payload2, r2.text)
        return (r.status_code, payload, r.text)
    except requests.RequestException as e:
        raise RuntimeError(f"Update driver error red: {e}")

def api_delete_driver(sess: requests.Session, cfg: SRConfig, user_id: int) -> int:
    base = ensure_base_url(cfg.base_url)
    url = base + f"accounts/drivers/{user_id}/"
    try:
        r = sess.delete(url, timeout=cfg.timeout)
        return r.status_code
    except requests.RequestException as e:
        print(f"[DELETE] error de red: {e}")
        return 0

def api_get_driver(sess: requests.Session, cfg: SRConfig, user_id: int) -> Tuple[int, Any]:
    base = ensure_base_url(cfg.base_url)
    url = base + f"accounts/drivers/{user_id}/"
    try:
        r = sess.get(url, timeout=cfg.timeout)
        try:
            payload = r.json()
        except Exception:
            payload = r.text
        return (r.status_code, payload)
    except requests.RequestException as e:
        return (0, f"GET error: {e}")

def api_patch_driver_fallback(sess: requests.Session, cfg: SRConfig, user_id: int, row: Dict[str, Any]) -> Tuple[int, Any, str]:
    """
    PATCH con fallback: intenta LISTA -> si ve 'Expected a dictionary', reintenta OBJETO.
    """
    base = ensure_base_url(cfg.base_url)
    url = base + f"accounts/drivers/{user_id}/"
    try:
        r = _patch_list(sess, url, [row], cfg.timeout)
        payload = _json_or_text(r)
        if r.status_code == 400 and isinstance(payload, dict) and any("Expected a dictionary" in str(v) for v in payload.values()):
            r2 = _patch_dict(sess, url, row, cfg.timeout)
            payload2 = _json_or_text(r2)
            return (r2.status_code, payload2, r2.text)
        return (r.status_code, payload, r.text)
    except requests.RequestException as e:
        return (0, {}, f"PATCH error: {e}")


# -------------------------------------------------------
# MODULO 3.5 : Helpers faltantes
# Objetivo - funciones
# 1) _safe_id_from_payload: extraer id desde list/dict
# 2) soft_deactivate_driver: inactivar por PUT con fallback (lista -> objeto)
# -------------------------------------------------------

def _safe_id_from_payload(payload):
    """
    Observaciones:
    1) La API puede responder [{...}] o {...}.
    2) Devuelve el entero 'id' o None si no está presente.
    """
    try:
        # Caso lista
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict) and "id" in first:
                return first["id"]
        # Caso dict
        if isinstance(payload, dict) and "id" in payload:
            return payload["id"]
    except Exception:
        pass
    return None

def soft_deactivate_driver(sess: requests.Session, cfg: SRConfig, user_id: int) -> Tuple[int, Any]:
    """
    Intenta desactivar/bloquear al driver con varios intentos:
    - Trae el objeto actual para heredar flags.
    - PUT y luego PATCH con payloads incrementales:
      a) {'is_driver': True, 'blocked': True, 'status': 'inactive'}
      b) + {'user_type': 'driver'} si el backend lo pide
      c) variantes sólo 'blocked' o sólo 'status' por compatibilidad
    Retorna (status_code_final, detalle_json/texto).
    """
    # 1) Prefetch
    st, current = api_get_driver(sess, cfg, user_id)
    if st != 200 or not isinstance(current, dict):
        # seguimos, pero partimos de payload mínimo
        current = {}

    # Base flags (conservamos banderas si existen)
    base_body = {
        "username": current.get("username", ""),
        "name": current.get("name", ""),
        "phone": current.get("phone", ""),
        "email": current.get("email", ""),
        # MUY IMPORTANTE para este tenant:
        "is_driver": True,  # fuerza tipo de usuario
        # preserva flags si están
        "is_admin": bool(current.get("is_admin", False)),
        "is_router": bool(current.get("is_router", False)),
        "is_monitor": bool(current.get("is_monitor", False)),
        "is_seller": bool(current.get("is_seller", False)),
        "is_seller_viewer": bool(current.get("is_seller_viewer", False)),
    }

    # Intentos ordenados (PUT primero, luego PATCH), con variantes
    attempts = [
        # completo con status y blocked
        {"method": "PUT",   "body": dict(base_body, blocked=True, status="inactive")},
        {"method": "PATCH", "body": dict(base_body, blocked=True, status="inactive")},
        # si pide user_type
        {"method": "PUT",   "body": dict(base_body, blocked=True, status="inactive", user_type="driver")},
        {"method": "PATCH", "body": dict(base_body, blocked=True, status="inactive", user_type="driver")},
        # sólo blocked
        {"method": "PUT",   "body": dict(base_body, blocked=True)},
        {"method": "PATCH", "body": dict(base_body, blocked=True)},
        # sólo status
        {"method": "PUT",   "body": dict(base_body, status="inactive")},
        {"method": "PATCH", "body": dict(base_body, status="inactive")},
    ]

    for att in attempts:
        if att["method"] == "PUT":
            stp, payload, raw = api_update_driver(sess, cfg, user_id, att["body"])
        else:
            stp, payload, raw = api_patch_driver_fallback(sess, cfg, user_id, att["body"])

        # éxito común
        if stp in (200, 202):
            return (stp, payload)

        # si devuelve explícitamente que falta tipo de usuario, pasamos a la variante con user_type
        text = raw if isinstance(raw, str) else str(raw)
        if "must select user type" in text.lower():
            continue  # el siguiente intento ya lo incluye

        # algunos tenants responden 400 pero aplican cambios; intentamos GET para verificar estado
        if stp == 400:
            gst, gobj = api_get_driver(sess, cfg, user_id)
            if gst == 200 and isinstance(gobj, dict):
                if gobj.get("blocked") is True or str(gobj.get("status","")).lower() == "inactive":
                    return (200, gobj)

    # último intento: ver estado final por GET y devolverlo
    gst, gobj = api_get_driver(sess, cfg, user_id)
    return (gst, gobj)



# -------------------------------------------------------
# MODULO 4 : Lectura/Validación de Excel
# - Estructuras requeridas
# - Validaciones de campos y sellers/fleets
# -------------------------------------------------------

def _to_bool(v) -> bool:
    return str(v).strip().upper() in ("TRUE","VERDADERO","1","SI","SÍ","YES")

def _norm_status(v: str) -> str:
    s = str(v).strip().lower()
    if s in ("", "none", "nan"):
        return "active"
    if s in ("active", "activo", "activa", "1", "true", "verdadero", "si", "sí", "yes"):
        return "active"
    if s in ("inactive", "inactivo", "inactiva", "0", "false", "falso", "no"):
        return "inactive"
    # fallback: si llega algo raro, lo dejamos activo para no bloquear sin querer
    return "active"

# Mínimos estrictos para WEB (lo demás se autorrellena si falta)
WEB_MIN_REQUIRED = ["email","name","role","read_only","temp_password"]

WEB_OPTIONAL = [
    "phone",
    "sellers_allow_csv",
    "fleets_allow_csv",
    "zones_allow_csv",   # interno (unificamos zone_allow_csv -> zones_allow_csv)
    "can_create_routes",
    "status",
    "require_2fa",
    "blocked",
    "notes",
]

# Opcionales soportados (se crean vacíos si no vienen)
WEB_WRITE_COLUMNS = [
    "email","name","phone","role",
    "sellers_allow_csv","fleets_allow_csv","zone_allow_csv",  # OJO: 'zone' singular aquí
    "read_only","can_create_routes","status","require_2fa","blocked",
    "temp_password","notes"
]

# Para MOBILE dejo tus mínimos originales
MOBILE_REQUIRED = [
    "username","name","phone","is_driver","is_codriver","blocked","status","plate","notes","temp_password"
]

def read_web_users(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró Excel WEB: {path}")
    # 1) intenta 'web_users'; si no existe, primera hoja
    try:
        df = pd.read_excel(path, sheet_name="web_users")
    except Exception:
        df = pd.read_excel(path)

    # columnas mínimas que esperas tener
    WEB_MIN_REQUIRED = [
        "email","name","role","sellers_allow_csv","fleets_allow_csv",
        "read_only","temp_password"
    ]
    missing = [c for c in WEB_MIN_REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"[WEB] Faltan columnas mínimas: {missing}")

    # alias zone_allow_csv -> zones_allow_csv
    if "zone_allow_csv" in df.columns and "zones_allow_csv" not in df.columns:
        df = df.rename(columns={"zone_allow_csv": "zones_allow_csv"})

    # columnas opcionales
    WEB_OPTIONAL = [
        "phone","notes","zones_allow_csv","can_create_routes","status","require_2fa","blocked"
    ]
    for col in WEB_OPTIONAL:
        if col not in df.columns:
            # booleans por default a False, strings vacías
            df[col] = False if col in ("read_only","can_create_routes","require_2fa","blocked") else ""

    # normalizaciones
    df["email"]   = df["email"].astype(str).str.strip()
    df["name"]    = df["name"].astype(str).str.strip()
    df["phone"]   = df["phone"].astype(str).str.strip()
    df["role"]    = df["role"].astype(str).str.strip().str.lower()
    df["notes"]   = df["notes"].astype(str)

    for col in ("sellers_allow_csv","fleets_allow_csv","zones_allow_csv"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).apply(
                lambda s: ",".join([x.strip() for x in s.split(",") if x.strip()])
            )

    df["read_only"]         = df["read_only"].apply(_to_bool)
    df["can_create_routes"] = df["can_create_routes"].apply(_to_bool)
    df["require_2fa"]       = df["require_2fa"].apply(_to_bool)
    df["blocked"]           = df["blocked"].apply(_to_bool)

    # status -> normalizado
    if "status" not in df.columns:
        df["status"] = "active"
    df["status"] = df["status"].apply(_norm_status)

    return df

def read_mobile_users(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró Excel MÓVIL: {path}")
    df = pd.read_excel(path, sheet_name="mobile_users")
    missing = [c for c in MOBILE_REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"[MOBILE] Faltan columnas: {missing}")

    # normalizaciones
    for col in ("username","name","status","plate","notes","temp_password"):
        df[col] = df[col].astype(str).str.strip()
    for col in ("is_driver","is_codriver","blocked"):
        df[col] = df[col].apply(_to_bool)
    df["phone"] = df["phone"].fillna("").astype(str).str.strip()
    return df

def maybe_read_sellers() -> Optional[pd.DataFrame]:
    if SELLERS_XLSX.exists():
        try:
            df = pd.read_excel(SELLERS_XLSX)
            if "seller_code" in df.columns:
                return df
        except Exception:
            return None
    return None

def validate_web(df: pd.DataFrame, sellers_df: Optional[pd.DataFrame]) -> List[str]:
    errors = []
    # email con @
    bad_email = df[~df["email"].str.contains("@")]
    if not bad_email.empty:
        errors.append(f"[WEB] emails inválidos: filas {list(bad_email.index.values)}")

    # roles soportados (ajusta si tu tenant maneja otros)
    valid_roles = {"admin","router","monitor","seller","seller_viewer","viewer"}
    bad_roles = df[~df["role"].isin(valid_roles)]
    if not bad_roles.empty:
        errors.append(f"[WEB] roles desconocidos: filas {list(bad_roles.index.values)} (permitidos={sorted(valid_roles)})")

    # status
    bad_status = df[~df["status"].isin(["active","inactive"])]
    if not bad_status.empty:
        errors.append(f"[WEB] status inválido (use active/inactive): filas {list(bad_status.index.values)}")

    # sellers permitidos (si hay config_sellers.xlsx)
    if sellers_df is not None:
        valid = set(sellers_df["seller_code"].astype(str).str.strip().unique())
        for i, row in df.iterrows():
            codes = [c.strip() for c in str(row["sellers_allow_csv"]).split(",") if c.strip()]
            bad = [c for c in codes if c not in valid]
            if bad:
                errors.append(f"[WEB] fila {i}: sellers no encontrados en config_sellers.xlsx -> {bad}")

    return errors

def validate_mobile(df: pd.DataFrame) -> List[str]:
    errors = []
    if not df[df["username"]==""].empty:
        errors.append(f"[MOBILE] username vacío en filas {list(df[df['username']=='' ].index.values)}")
    bad_status = df[~df["status"].isin(["active","inactive"])]
    if not bad_status.empty:
        errors.append(f"[MOBILE] status inválido (use active/inactive): filas {list(bad_status.index.values)}")
    bad_driver = df[~df["is_driver"]]
    if not bad_driver.empty:
        errors.append(f"[MOBILE] is_driver debe ser VERDADERO: filas {list(bad_driver.index.values)}")
    return errors


# -------------------------------------------------------
# MODULO 5 : Transformaciones de payload
# - mapear web/mobile a body API
# -------------------------------------------------------

def body_from_mobile_row(row: pd.Series) -> Dict[str, Any]:
    body = {
        "username": row["username"],
        "name": row["name"],
        "phone": row.get("phone",""),
        "email": "",
        "is_admin": False,
        "password": row.get("temp_password","") or None,
        "is_driver": True
    }
    if row.get("plate"):
        body["name"] = f"{row['name']} [{row['plate']}]"
    return body

def body_from_web_row(row: pd.Series) -> Dict[str, Any]:
    role = str(row["role"]).lower()
    flags = {
        "is_admin": False,
        "is_router": False,
        "is_monitor": False,
        "is_seller": False,
        "is_seller_viewer": False,
    }
    if role == "admin":
        flags["is_admin"] = True
    elif role == "router":
        flags["is_router"] = True
    elif role == "monitor":
        flags["is_monitor"] = True
    elif role == "seller":
        flags["is_seller"] = True
    elif role == "seller_viewer":
        flags["is_seller_viewer"] = True
    else:
        flags["is_monitor"] = True  # viewer básico

    body = {
        "username": row["email"],
        "name": row["name"],
        "phone": "",
        "email": row["email"],
        "password": row.get("temp_password","") or None,
        "is_driver": False,
    }
    body.update(flags)
    return body

# -------------------------------------------------------
# MODULO 6 : Lógica de UPSERT
# - buscar por username en la lista actual
# - crear o actualizar según corresponda
# -------------------------------------------------------

def index_by_username(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for u in users:
        uname = str(u.get("username","")).strip().lower()
        if uname:
            out[uname] = u
    return out

def upsert_mobile(sess: requests.Session, cfg: SRConfig, df: pd.DataFrame, existing_by_username: Dict[str, Any]) -> Tuple[int,int,int]:
    created = updated = skipped = 0
    to_create = []
    for _, row in df.iterrows():
        uname = str(row["username"]).strip().lower()
        body = body_from_mobile_row(row)
        if uname in existing_by_username:
            user_id = existing_by_username[uname]["id"]
            status, payload, raw = api_update_driver(sess, cfg, user_id, body)
            if status in (200, 201):
                updated += 1
                print(f"[MOBILE][UPDATE] {uname} -> id={user_id}")
            else:
                print(f"[MOBILE][WARN] PUT {uname} -> {status} {str(raw)[:160]}")
                skipped += 1
        else:
            to_create.append(body)
    if to_create:
        status, payload, raw = api_create_drivers(sess, cfg, to_create)
        if status in (200,201):
            created += len(to_create)
            # imprimir ids si vinieron
            if isinstance(payload, list):
                for obj in payload:
                    print(f"[MOBILE][CREATE] id={obj.get('id')} username={obj.get('username')}")
        else:
            print(f"[MOBILE][ERROR] POST batch -> {status} {str(raw)[:200]}")
            skipped += len(to_create)
    return (created, updated, skipped)

def upsert_web(sess: requests.Session, cfg: SRConfig, df: pd.DataFrame, existing_by_username: Dict[str, Any]) -> Tuple[int,int,int,int]:
    created = updated = skipped = failed = 0
    to_create = []
    for _, row in df.iterrows():
        uname = str(row["email"]).strip().lower()
        body = body_from_web_row(row)
        if uname in existing_by_username:
            user_id = existing_by_username[uname]["id"]
            status, payload, raw = api_update_driver(sess, cfg, user_id, body)
            if status in (200,201):
                updated += 1
                print(f"[WEB][UPDATE] {uname} -> id={user_id}")
            else:
                print(f"[WEB][WARN] PUT {uname} -> {status} {str(raw)[:160]}")
                skipped += 1
        else:
            to_create.append(body)
    if to_create:
        if not cfg.try_web_api:
            failed += len(to_create)
            print("[WEB] Creación por API deshabilitada (--no-try-web).")
        else:
            status, payload, raw = api_create_drivers(sess, cfg, to_create)
            if status in (200,201):
                created += len(to_create)
                if isinstance(payload, list):
                    for obj in payload:
                        print(f"[WEB][CREATE] id={obj.get('id')} username={obj.get('username')}")
            else:
                failed += len(to_create)
                out = ROOT_DIR / "web_users_pending_creation.csv"
                pd.DataFrame(to_create).to_csv(out, index=False, encoding="utf-8")
                print(f"[WEB][ERROR] POST -> {status}. Ver {out}. Detalle: {str(raw)[:200]}")
    return (created, updated, skipped, failed)

# -------------------------------------------------------
# MODULO 7 : Comandos CLI
# - template: genera templates en raíz
# - validate: valida ambos excels
# - sync: upsert mobile y web (con fallback)
# - test: crea un usuario driver de prueba (con fallback) y limpia
# -------------------------------------------------------

TEMPLATE_WEB_MIN = [{
    "email":"usuario@dominio.com",
    "name":"Nombre Apellido",
    "role":"viewer",
    "sellers_allow_csv":"",
    "fleets_allow_csv":"",
    "read_only": True,
    "temp_password": "DRIMER2025VIEW"
}]

TEMPLATE_MOBILE_MIN = [{
    "username":"mov_BLR794",
    "name":"Unidad BLR-794",
    "phone":"",
    "is_driver":"VERDADERO",
    "is_codriver":"FALSO",
    "blocked":"FALSO",
    "status":"active",
    "plate":"BLR-794",
    "notes":"Usuario móvil asociado a la unidad; chofer/ayudantes se registran en la ruta",
    "temp_password":"DRIMER2025MOVIL"
}]

def cmd_template(args):
    web_path    = path_resolver(args.web_excel, DEFAULT_WEB_XLSX)
    mobile_path = path_resolver(args.mobile_excel, DEFAULT_MOBILE_XLSX)

    if not web_path.exists():
        # construye el template con las columnas que tú usas
        sample = [{
            "email":"usuario@dominio.com",
            "name":"Nombre Apellido",
            "phone":"",
            "role":"viewer",
            "sellers_allow_csv":"",
            "fleets_allow_csv":"",
            "zone_allow_csv":"",   # singular en archivo; dentro del código lo normalizamos
            "read_only": True,
            "can_create_routes": False,
            "status":"active",
            "require_2fa": False,
            "blocked": False,
            "temp_password":"DRIMER2025VIEW",
            "notes":""
        }]
        pd.DataFrame(sample, columns=WEB_WRITE_COLUMNS).to_excel(
            web_path, index=False, sheet_name="web_users"
        )
        print(f"[TEMPLATE] creado: {web_path}")
    else:
        print(f"[TEMPLATE] ya existe: {web_path}")

    if not mobile_path.exists():
        with pd.ExcelWriter(mobile_path, engine="xlsxwriter") as wr:
            pd.DataFrame(TEMPLATE_MOBILE_MIN, columns=MOBILE_REQUIRED).to_excel(
                wr, index=False, sheet_name="mobile_users"
            )
            readme = pd.DataFrame([
                ["username","Identificador de login móvil (no requiere email). Sugerido: mov_{PLACA}"],
                ["name","Nombre mostrado (recomendado: Unidad {PLACA})"],
                ["temp_password","Se intentará fijar como contraseña inicial vía API"],
                ["is_driver","Debe ser VERDADERO para móviles"],
                ["plate","Placa de la unidad (auditoría interna)"]
            ], columns=["Campo","Descripción"])
            readme.to_excel(wr, index=False, sheet_name="README")
        print(f"[TEMPLATE] creado: {mobile_path}")
    else:
        print(f"[TEMPLATE] ya existe: {mobile_path}")

def build_cfg_from_args(args) -> SRConfig:
    load_env()
    token = args.token or os.environ.get("SIMPLIROUTE_TOKEN") or ""
    if not token:
        token = input(">> Ingresa tu Token de SimpliRoute: ").strip()

    base_url = args.base_url or os.environ.get("SIMPLIROUTE_BASE_URL","https://api.simpliroute.com")

    # NEW: timeout configurable
    env_timeout = os.environ.get("SIMPLIROUTE_TIMEOUT", "").strip()
    cfg_timeout = int(env_timeout) if env_timeout.isdigit() else 30
    if getattr(args, "timeout", None):
        cfg_timeout = int(args.timeout)

    web_path    = path_resolver(args.web_excel, DEFAULT_WEB_XLSX)
    mobile_path = path_resolver(args.mobile_excel, DEFAULT_MOBILE_XLSX)
    print(f"[CFG] base_url={base_url}")
    print(f"[CFG] WEB   : {web_path}")
    print(f"[CFG] MOBILE: {mobile_path}")
    print(f"[CFG] timeout={cfg_timeout}s")

    return SRConfig(
        base_url=base_url, token=token,
        web_path=web_path, mobile_path=mobile_path,
        timeout=cfg_timeout, try_web_api=not args.no_try_web
    )

def cmd_validate(args):
    cfg = build_cfg_from_args(args)
    sellers_df = maybe_read_sellers()

    errors_all: List[str] = []
    if cfg.web_path.exists():
        dfw = read_web_users(cfg.web_path)
        errors_all += validate_web(dfw, sellers_df)
        print(f"[VALIDATE][WEB] filas={len(dfw)}")
    else:
        print(f"[VALIDATE][WEB] no encontrado: {cfg.web_path}")

    if cfg.mobile_path.exists():
        dfm = read_mobile_users(cfg.mobile_path)
        errors_all += validate_mobile(dfm)
        print(f"[VALIDATE][MOBILE] filas={len(dfm)}")
    else:
        print(f"[VALIDATE][MOBILE] no encontrado: {cfg.mobile_path}")

    if errors_all:
        print("---- ERRORES ----")
        for e in errors_all:
            print(" -", e)
        sys.exit(2)
    else:
        print("[VALIDATE] OK. Estructura y reglas básicas correctas.")

def cmd_sync(args):
    cfg = build_cfg_from_args(args)
    sess = make_session(cfg)
    if ping_auth(sess, cfg) != 200:
        print("[SYNC] Autenticación fallida. Revisa token/base_url en .env o parámetros.")
        sys.exit(2)

    # si no existe el archivo, crea un DF vacío con los mínimos (no rompe)
    dfw = read_web_users(cfg.web_path) if cfg.web_path.exists() \
         else pd.DataFrame(columns=WEB_MIN_REQUIRED)
    dfm = read_mobile_users(cfg.mobile_path) if cfg.mobile_path.exists() \
         else pd.DataFrame(columns=MOBILE_REQUIRED)

    existing    = api_list_drivers(sess, cfg)
    by_username = index_by_username(existing)

    created_m, updated_m, skipped_m = (0,0,0)
    if not args.mode or args.mode in ("both","mobile"):
        created_m, updated_m, skipped_m = upsert_mobile(sess, cfg, dfm, by_username)

    created_w, updated_w, skipped_w, failed_w = (0,0,0,0)
    if not args.mode or args.mode in ("both","web"):
        created_w, updated_w, skipped_w, failed_w = upsert_web(sess, cfg, dfw, by_username)

    print(f"[RESULT] MOBILE -> created={created_m} updated={updated_m} skipped={skipped_m}")
    print(f"[RESULT] WEB    -> created={created_w} updated={updated_w} skipped={skipped_w} failed_api={failed_w}")

def random_suffix(n=5):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))

def cmd_test(args):
    cfg = build_cfg_from_args(args)
    sess = make_session(cfg)
    if ping_auth(sess, cfg) != 200:
        print("[TEST] Autenticación fallida. Revisa token/base_url en .env o parámetros.")
        sys.exit(2)

    uname = f"mov_TESTAPI_{int(time.time())}_{random_suffix()}"
    row = pd.Series({
        "username": uname,
        "name": "Unidad TEST API",
        "phone": "",
        "is_driver": True,
        "is_codriver": False,
        "blocked": False,
        "status": "active",
        "plate": "TEST-000",
        "notes": "usuario de prueba",
        "temp_password": "DRIMER2025TEST"
    })
    body = body_from_mobile_row(row)

    # Crear con fallback (LISTA -> si 400 'Expected a dictionary', reintenta como OBJETO)
    status, payload, raw = api_create_driver_fallback(sess, cfg, body)
    if status not in (200, 201):
        print(f"[TEST] ERROR -> status={status} detail={str(raw)[:200]}")
        return

    user_id = _safe_id_from_payload(payload)
    print(f"[TEST] OK -> status={status} username={uname} id={user_id}")

    # limpieza salvo --keep-test-users
    keep = getattr(args, "keep_test_users", False)
    if not user_id or keep:
        return

    del_status = api_delete_driver(sess, cfg, user_id)
    if del_status in (200, 204):
        print(f"[TEST][CLEANUP] DELETE /accounts/drivers/{user_id}/ -> {del_status}")
    else:
        deact_status, deact_detail = soft_deactivate_driver(sess, cfg, user_id)
        print(f"[TEST][CLEANUP] DEACTIVATE /accounts/drivers/{user_id}/ -> {deact_status}")
        if deact_status >= 400:
            print(f"[TEST][CLEANUP][DETAIL] {str(deact_detail)[:300]}")

# -------------------------------------------------------
# MODULO 8 : Parser CLI (pensado para futura GUI)
# - Subcomandos y parámetros
# -------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CLI de Usuarios (Web & Móvil) - SimpliRoute")
    p.add_argument("--base-url", help="Base URL de la API (default: https://api.simpliroute.com)", default=None)
    p.add_argument("--token", help="Token SR (si no, usa SIMPLIROUTE_TOKEN de .env)", default=None)
    p.add_argument("--web-excel", help="Ruta Excel WEB (default: raiz/config_web_users.xlsx)", default=None)
    p.add_argument("--mobile-excel", help="Ruta Excel MÓVIL (default: raiz/config_mobile_users.xlsx)", default=None)
    p.add_argument("--mode", choices=["both","web","mobile"], help="Qué sincronizar (default: both)", default="both")
    p.add_argument("--no-try-web", action="store_true", help="No intentar crear web users por API (solo validar/reportar)")
    p.add_argument("--keep-test-users", action="store_true", help="No elimina el usuario de prueba creado en 'test'")
    p.add_argument("--timeout", type=int, default=None, help="Timeout en segundos para requests (default: 30, o SIMPLIROUTE_TIMEOUT)")

    sp = p.add_subparsers(dest="cmd", required=True)
    sp_t = sp.add_parser("template", help="Generar templates mínimos en raíz")
    sp_t.set_defaults(func=cmd_template)

    sp_v = sp.add_parser("validate", help="Validar excels de web y móvil")
    sp_v.set_defaults(func=cmd_validate)

    sp_s = sp.add_parser("sync", help="Sincronizar usuarios (upsert). Requiere token válido")
    sp_s.set_defaults(func=cmd_sync)

    sp_test = sp.add_parser("test", help="Crear un usuario móvil de prueba para validar conectividad")
    sp_test.set_defaults(func=cmd_test)

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
