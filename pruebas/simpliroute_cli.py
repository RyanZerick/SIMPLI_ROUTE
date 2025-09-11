#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SimpliRoute API Quick Tester (CLI)
Autor: Brian + ChatGPT
Requisitos: Python 3.8+, 'requests' (pip install requests)
Uso básico:
  export SIMPLIROUTE_TOKEN="tu_token"
  python simpliroute_cli.py auth
  python simpliroute_cli.py vehicles-create --name "Camión 1" --capacity 1000
  python simpliroute_cli.py clients-create --key C001 --title "Cliente 1" --address "Lima, PE"
  python simpliroute_cli.py users-create --username conductor01 --name "Conductor 01" --is_driver true --password "Secreto123"
Nota: TODOS los comandos aceptan --token y --base-url. Si no los pasas, se te preguntará y/o se leerá del entorno.
"""
import os
import sys
import json
import base64
import argparse
import datetime as dt
from typing import Any, Dict, Optional, Tuple

try:
    import requests  # type: ignore
except ImportError as e:
    print("Falta la librería 'requests'. Instálala con: pip install requests", file=sys.stderr)
    raise

API_DEFAULT_BASE = "https://api.simpliroute.com"

# -------------------------------------------------------
# MODULO 1 : Utilidades y Configuración (HTTP / CLI)
# Objetivo: Normalizar peticiones; manejo de token/base_url; helpers CLI.
# -------------------------------------------------------
# Observaciones:
# 1) http_request: arma headers, envía método HTTP, imprime trazas si --verbose.
# 2) resolve_token: prioriza CLI > ENV(SIMPLIROUTE_TOKEN) > input interactivo.
# 3) prompt_if_none: solicita parámetros si faltan (para defaults del CLI).
# 4) pretty: imprime JSON legible; fallback a texto si no es JSON.
# 5) build_parser: define subcomandos para cada endpoint probado.
def http_request(
    method: str,
    path: str,
    token: str,
    base_url: str = API_DEFAULT_BASE,
    timeout: int = 30,
    json_body: Optional[Dict[str, Any]] = None,
    data: Any = None,
    files: Any = None,
    verbose: bool = False,
) -> Tuple[int, Any]:
    url = path if path.startswith("http") else f"{base_url.rstrip('/')}{path}"
    headers = {"Authorization": f"Token {token}"}
    if files is None:
        headers["Content-Type"] = "application/json"

    if verbose:
        print(f"[HTTP] {method} {url}")
        print(f"[HTTP] headers: { {k: ('***' if k.lower()=='authorization' else v) for k,v in headers.items()} }")
        if json_body is not None:
            print(f"[HTTP] json: {json.dumps(json_body, ensure_ascii=False, indent=2)}")
        if data is not None and files is None:
            try:
                print(f"[HTTP] data: {json.dumps(json.loads(data), ensure_ascii=False, indent=2)}")
            except Exception:
                print(f"[HTTP] data(raw): {data}")

    resp = requests.request(
        method=method.upper(),
        url=url,
        headers=headers,
        json=json_body,
        data=data,
        files=files,
        timeout=timeout,
    )
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, resp.text


def resolve_token(cli_token: Optional[str]) -> str:
    token = cli_token or os.getenv("SIMPLIROUTE_TOKEN")
    if not token:
        token = input("Ingresa tu Token de SimpliRoute: ").strip()
    return token


def prompt_if_none(value: Optional[str], prompt: str, default: Optional[str] = None) -> str:
    if value is not None:
        return value
    text = f"{prompt}"
    if default is not None:
        text += f" [{default}]"
    text += ": "
    ans = input(text).strip()
    return ans or (default or "")


def pretty(status: int, payload: Any) -> None:
    print(f"\n[STATUS] {status}")
    try:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    except Exception:
        print(payload)


# -------------------------------------------------------
# MODULO 2 : Autenticación (GET /v1/accounts/me/)
# Objetivo: Probar conexión y validez del token.
# -------------------------------------------------------
# Observaciones:
# 1) auth_me: GET /v1/accounts/me/ para verificar credenciales.
def auth_me(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/accounts/me/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 3 : Vehículos (Vehicles)
# Objetivo: Crear/Listar/Eliminar vehículos y validar campos (capacidad, placas, turnos, skills).
# -------------------------------------------------------
# Observaciones:
# 1) vehicles_create: POST /v1/routes/vehicles/
# 2) vehicles_list: GET  /v1/routes/vehicles/
# 3) vehicles_delete: DELETE /v1/routes/vehicles/{id}/
def vehicles_create(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    name = prompt_if_none(args.name, "Nombre del vehículo", "Camión-1")
    capacity = float(prompt_if_none(args.capacity, "Capacidad (load_1)", "1000"))
    body = {
        "name": name,
        "capacity": capacity,
        "default_driver": args.default_driver,
        "location_start_address": args.location_start_address,
        "location_end_address": args.location_end_address,
        "location_end_latitude": args.location_end_latitude,
        "location_end_longitude": args.location_end_longitude,
        "location_start_latitude": args.location_start_latitude,
        "location_start_longitude": args.location_start_longitude,
        "skills": args.skills or [],
        "capacity2": args.capacity2,
        "capacity3": args.capacity3,
        "cost": args.cost,
        "shift_start": args.shift_start,
        "shift_end": args.shift_end,
        "reference_id": args.reference_id,
        "license_plate": args.license_plate,
        "min_load": args.min_load,
        "min_load_2": args.min_load_2,
        "min_load_3": args.min_load_3,
        "max_visit": args.max_visit,
        "max_time": args.max_time,
        "rest_time_start": args.rest_time_start,
        "rest_time_end": args.rest_time_end,
        "rest_time_duration": args.rest_time_duration,
        "codrivers": args.codrivers or [],
    }
    status, payload = http_request("POST", "/v1/routes/vehicles/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


def vehicles_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/routes/vehicles/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def vehicles_delete(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    vid = prompt_if_none(args.vehicle_id, "ID de vehículo a eliminar")
    status, payload = http_request("DELETE", f"/v1/routes/vehicles/{vid}/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 4 : Clientes (Clients)
# Objetivo: Crear/Obtener/Actualizar/Eliminar clientes + properties personalizadas.
# -------------------------------------------------------
# Observaciones:
# 1) clients_create: POST /v1/accounts/clients/
# 2) clients_get: GET /v1/accounts/clients/?key=
# 3) clients_update_bulk: PUT /v1/accounts/clients/ (array)
# 4) clients_delete_one: DELETE /v1/accounts/clients/{id}/
# 5) clients_delete_bulk:  DELETE /v1/accounts/clients/ (array en body)
def clients_create(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    key = prompt_if_none(args.key, "Key/ID del cliente (único)", "C001")
    title = prompt_if_none(args.title, "Título/Nombre del cliente", "Cliente Demo")
    address = prompt_if_none(args.address, "Dirección", "Lima, PE")
    body = [{
        "key": key,
        "title": title,
        "address": address,
        "latitude": args.latitude,
        "longitude": args.longitude,
        "load": args.load,
        "load_2": args.load_2,
        "load_3": args.load_3,
        "window_start": args.window_start,
        "window_end": args.window_end,
        "window_start_2": args.window_start_2,
        "window_end_2": args.window_end_2,
        "duration": args.duration,
        "contact_name": args.contact_name,
        "contact_phone": args.contact_phone,
        "contact_email": args.contact_email,
        "notes": args.notes,
        "priority_level": args.priority_level,
        "skills_required": args.skills_required or [],
        "skills_optional": args.skills_optional or [],
        "tags": args.tags or [],
        "visit_type": args.visit_type,
        "custom_properties": json.loads(args.custom_properties_json) if args.custom_properties_json else None
    }]
    status, payload = http_request("POST", "/v1/accounts/clients/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


def clients_get(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    key = prompt_if_none(args.key, "Key/ID del cliente a obtener")
    status, payload = http_request("GET", f"/v1/accounts/clients/?key={key}", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def clients_update_bulk(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    # Permite pasar un archivo JSON con el array de clientes a actualizar.
    if args.file and os.path.exists(args.file):
        with open(args.file, "r", encoding="utf-8") as f:
            body = json.load(f)
    else:
        raise SystemExit("Debes pasar --file con un JSON array de clientes para PUT masivo.")
    status, payload = http_request("PUT", "/v1/accounts/clients/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


def clients_delete_one(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    cid = prompt_if_none(args.client_id, "ID del cliente a eliminar")
    status, payload = http_request("DELETE", f"/v1/accounts/clients/{cid}/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def clients_delete_bulk(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    ids = args.ids or []
    if not ids:
        ids_str = prompt_if_none(None, "IDs a eliminar (separados por coma)", "")
        ids = [int(x.strip()) for x in ids_str.split(",") if x.strip()]
    body = ids
    # DELETE con body (según docs), algunas librerías requieren method override
    status, payload = http_request("DELETE", "/v1/accounts/clients/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 5 : Propiedades Personalizadas de Cliente
# Objetivo: Crear atributos (label/type) para enriquecer cliente y segmentar.
# -------------------------------------------------------
# Observaciones:
# 1) client_property_create: POST /v1/planner/client-properties/
def client_property_create(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    label = prompt_if_none(args.label, "Etiqueta del atributo", "segmento")
    dtype = prompt_if_none(args.type, "Tipo (str|int|float|bool)", "str")
    body = {"label": label, "type": dtype}
    status, payload = http_request("POST", "/v1/planner/client-properties/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 6 : Usuarios (Drivers/Web)
# Objetivo: Crear/Listar/Actualizar/Eliminar usuarios (móvil y web por flags).
# -------------------------------------------------------
# Observaciones:
# 1) users_list:  GET /v1/accounts/drivers/
# 2) users_create: POST /v1/accounts/drivers/ (is_driver, is_admin, is_monitor, etc.)
# 3) users_get:   GET /v1/accounts/drivers/{id}/
# 4) users_update:PUT /v1/accounts/drivers/{id}/
# 5) users_delete:DELETE /v1/accounts/drivers/{id}/
def users_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/accounts/drivers/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def users_create(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    username = prompt_if_none(args.username, "Username", "usuario01")
    name = prompt_if_none(args.name, "Nombre completo", "Usuario Demo")
    password = prompt_if_none(args.password, "Password (se enviará tal cual)", "CambioMe1!")
    body = [{
        "username": username,
        "name": name,
        "phone": args.phone,
        "email": args.email,
        "is_admin": str(args.is_admin).lower() == "true",
        "is_driver": str(args.is_driver).lower() == "true",
        "is_monitor": str(args.is_monitor).lower() == "true",
        "is_router": str(args.is_router).lower() == "true",
        "is_coordinator": str(args.is_coordinator).lower() == "true",
        "password": password,
    }]
    status, payload = http_request("POST", "/v1/accounts/drivers/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


def users_get(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    uid = prompt_if_none(args.user_id, "ID del usuario")
    status, payload = http_request("GET", f"/v1/accounts/drivers/{uid}/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def users_update(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    uid = prompt_if_none(args.user_id, "ID del usuario a actualizar")
    body = [{
        "username": args.username,
        "name": args.name,
        "email": args.email,
        "is_admin": str(args.is_admin).lower() == "true" if args.is_admin is not None else None,
        "password": args.password,
    }]
    # limpiar None para PUT limpio
    body = [{k: v for k, v in body[0].items() if v is not None}]
    status, payload = http_request("PUT", f"/v1/accounts/drivers/{uid}/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


def users_delete(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    uid = prompt_if_none(args.user_id, "ID del usuario a eliminar")
    status, payload = http_request("DELETE", f"/v1/accounts/drivers/{uid}/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 7 : ExtraFields (definiciones y valores)
# Objetivo: Leer definiciones y guardar valores de extrafields en visitas.
# -------------------------------------------------------
# Observaciones:
# 1) extra_fields_list: GET /v1/accounts/extra-fields/
# 2) visits_set_extrafields: POST /v1/routes/visits/multiple/extra-field-values
def extra_fields_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/accounts/extra-fields/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


def visits_set_extrafields(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    visits = args.visits or []
    if not visits:
        visits_str = prompt_if_none(None, "IDs de visitas (separados por coma)", "")
        visits = [int(x.strip()) for x in visits_str.split(",") if x.strip()]
    # 'json' debe ser un string JSON según docs
    json_payload_str = args.json_string or '{"ejemplo": true}'
    body = {"json": json_payload_str, "visits": visits}
    status, payload = http_request("POST", "/v1/routes/visits/multiple/extra-field-values", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 8 : Skills
# Objetivo: Listar skills disponibles (para vehículos/visitas).
# -------------------------------------------------------
# Observaciones:
# 1) skills_list: GET /v1/routes/skills/
def skills_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/routes/skills/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 9 : Zones
# Objetivo: Listar zonas (si el módulo está habilitado en la cuenta).
# -------------------------------------------------------
# Observaciones:
# 1) zones_list: GET /v1/zones/
def zones_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/zones/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 10 : Fleets
# Objetivo: Listar flotas y conocer asociación vehículos/usuarios.
# -------------------------------------------------------
# Observaciones:
# 1) fleets_list: GET /v1/fleets/
def fleets_list(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    status, payload = http_request("GET", "/v1/fleets/", token, base_url, timeout=args.timeout, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 11 : Checkout de Visita (firma digital)
# Objetivo: Completar entrega con firma base64 (y ubicación).
# -------------------------------------------------------
# Observaciones:
# 1) visits_checkout: POST /v1/mobile/visit/{visit_id}/checkout/
# 2) Encode de imagen local a base64 para 'signature' (opcional).
def visits_checkout(args: argparse.Namespace) -> None:
    token = resolve_token(args.token)
    base_url = args.base_url or API_DEFAULT_BASE
    visit_id = prompt_if_none(args.visit_id, "ID de la visita a checkout")
    status_str = args.status or "completed"
    now_iso = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    checkout_time = args.checkout_time or now_iso
    lat = float(args.lat) if args.lat is not None else -12.046374
    lon = float(args.lon) if args.lon is not None else -77.042793
    signature_b64 = None
    if args.signature_path:
        with open(args.signature_path, "rb") as f:
            signature_b64 = base64.b64encode(f.read()).decode("utf-8")
    body = {
        "status": status_str,
        "checkout_time": checkout_time,
        "checkout_latitude": lat,
        "checkout_longitude": lon,
        "checkout_comment": args.comment,
    }
    if signature_b64:
        body["signature"] = signature_b64
    status, payload = http_request("POST", f"/v1/mobile/visit/{visit_id}/checkout/", token, base_url, timeout=args.timeout, json_body=body, verbose=args.verbose)
    pretty(status, payload)


# -------------------------------------------------------
# MODULO 12 : CLI (argparse)
# Objetivo: Armar subcomandos y parámetros con defaults/prompt.
# -------------------------------------------------------
# Observaciones:
# 1) Cada subcomando mapea a una función de las anteriores.
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="simpliroute-cli", description="Tester rápido de API SimpliRoute")
    p.add_argument("--base-url", default=API_DEFAULT_BASE, help="Base URL (default: https://api.simpliroute.com)")
    p.add_argument("--token", help="Token (o usa ENV SIMPLIROUTE_TOKEN)")
    p.add_argument("--timeout", type=int, default=30, help="Timeout en segundos (default: 30)")
    p.add_argument("--verbose", action="store_true", help="Trazas HTTP")

    sp = p.add_subparsers(dest="cmd", required=True)

    # AUTH
    sp_auth = sp.add_parser("auth", help="Probar token (GET /v1/accounts/me/)")
    sp_auth.set_defaults(func=auth_me)

    # VEHICLES
    sp_vc = sp.add_parser("vehicles-create", help="Crear vehículo")
    sp_vc.add_argument("--name")
    sp_vc.add_argument("--capacity")
    sp_vc.add_argument("--default-driver", dest="default_driver", type=int)
    sp_vc.add_argument("--location-start-address")
    sp_vc.add_argument("--location-end-address")
    sp_vc.add_argument("--location-start-latitude")
    sp_vc.add_argument("--location-start-longitude")
    sp_vc.add_argument("--location-end-latitude")
    sp_vc.add_argument("--location-end-longitude")
    sp_vc.add_argument("--skills", nargs="*", type=int)
    sp_vc.add_argument("--capacity2")
    sp_vc.add_argument("--capacity3")
    sp_vc.add_argument("--cost")
    sp_vc.add_argument("--shift-start")
    sp_vc.add_argument("--shift-end")
    sp_vc.add_argument("--reference-id")
    sp_vc.add_argument("--license-plate")
    sp_vc.add_argument("--min-load", dest="min_load", type=float)
    sp_vc.add_argument("--min-load-2", dest="min_load_2", type=float)
    sp_vc.add_argument("--min-load-3", dest="min_load_3", type=float)
    sp_vc.add_argument("--max-visit", dest="max_visit", type=int)
    sp_vc.add_argument("--max-time", dest="max_time")
    sp_vc.add_argument("--rest-time-start")
    sp_vc.add_argument("--rest-time-end")
    sp_vc.add_argument("--rest-time-duration")
    sp_vc.add_argument("--codrivers", nargs="*", type=int)
    sp_vc.set_defaults(func=vehicles_create)

    sp_vl = sp.add_parser("vehicles-list", help="Listar vehículos")
    sp_vl.set_defaults(func=vehicles_list)

    sp_vd = sp.add_parser("vehicles-delete", help="Eliminar vehículo por ID")
    sp_vd.add_argument("--vehicle-id")
    sp_vd.set_defaults(func=vehicles_delete)

    # CLIENTS
    sp_cc = sp.add_parser("clients-create", help="Crear cliente")
    sp_cc.add_argument("--key")
    sp_cc.add_argument("--title")
    sp_cc.add_argument("--address")
    sp_cc.add_argument("--latitude", type=float)
    sp_cc.add_argument("--longitude", type=float)
    sp_cc.add_argument("--load", type=float)
    sp_cc.add_argument("--load-2", dest="load_2", type=float)
    sp_cc.add_argument("--load-3", dest="load_3", type=float)
    sp_cc.add_argument("--window-start")
    sp_cc.add_argument("--window-end")
    sp_cc.add_argument("--window-start-2", dest="window_start_2")
    sp_cc.add_argument("--window-end-2", dest="window_end_2")
    sp_cc.add_argument("--duration", default="00:00:10")
    sp_cc.add_argument("--contact-name", dest="contact_name")
    sp_cc.add_argument("--contact-phone", dest="contact_phone")
    sp_cc.add_argument("--contact-email", dest="contact_email")
    sp_cc.add_argument("--notes")
    sp_cc.add_argument("--priority-level", dest="priority_level", type=int)
    sp_cc.add_argument("--skills-required", dest="skills_required", nargs="*", type=int)
    sp_cc.add_argument("--skills-optional", dest="skills_optional", nargs="*", type=int)
    sp_cc.add_argument("--tags", nargs="*", type=int)
    sp_cc.add_argument("--visit-type", dest="visit_type")
    sp_cc.add_argument("--custom-properties-json", dest="custom_properties_json", help='JSON como string: {"segmento":"A"}')
    sp_cc.set_defaults(func=clients_create)

    sp_cg = sp.add_parser("clients-get", help="Obtener cliente por key")
    sp_cg.add_argument("--key")
    sp_cg.set_defaults(func=clients_get)

    sp_cu = sp.add_parser("clients-update-bulk", help="Actualizar clientes (PUT masivo desde archivo JSON)")
    sp_cu.add_argument("--file", required=True, help="Ruta a archivo JSON (array)")
    sp_cu.set_defaults(func=clients_update_bulk)

    sp_cd1 = sp.add_parser("clients-delete-one", help="Eliminar UN cliente por ID")
    sp_cd1.add_argument("--client-id")
    sp_cd1.set_defaults(func=clients_delete_one)

    sp_cdb = sp.add_parser("clients-delete-bulk", help="Eliminar clientes por IDs (array)")
    sp_cdb.add_argument("--ids", nargs="*", type=int)
    sp_cdb.set_defaults(func=clients_delete_bulk)

    # CLIENT PROPERTIES
    sp_cp = sp.add_parser("client-property-create", help="Crear propiedad personalizada de cliente")
    sp_cp.add_argument("--label")
    sp_cp.add_argument("--type", choices=["str", "int", "float", "bool"])
    sp_cp.set_defaults(func=client_property_create)

    # USERS
    sp_ul = sp.add_parser("users-list", help="Listar usuarios")
    sp_ul.set_defaults(func=users_list)

    sp_uc = sp.add_parser("users-create", help="Crear usuario (driver/web por flags)")
    sp_uc.add_argument("--username")
    sp_uc.add_argument("--name")
    sp_uc.add_argument("--email")
    sp_uc.add_argument("--phone")
    sp_uc.add_argument("--password")
    sp_uc.add_argument("--is_driver", default="true")
    sp_uc.add_argument("--is_admin", default="false")
    sp_uc.add_argument("--is_monitor", default="false")
    sp_uc.add_argument("--is_router", default="false")
    sp_uc.add_argument("--is_coordinator", default="false")
    sp_uc.set_defaults(func=users_create)

    sp_ug = sp.add_parser("users-get", help="Obtener un usuario por ID")
    sp_ug.add_argument("--user-id")
    sp_ug.set_defaults(func=users_get)

    sp_uu = sp.add_parser("users-update", help="Actualizar usuario por ID")
    sp_uu.add_argument("--user-id")
    sp_uu.add_argument("--username")
    sp_uu.add_argument("--name")
    sp_uu.add_argument("--email")
    sp_uu.add_argument("--password")
    sp_uu.add_argument("--is_admin")
    sp_uu.set_defaults(func=users_update)

    sp_ud = sp.add_parser("users-delete", help="Eliminar usuario por ID")
    sp_ud.add_argument("--user-id")
    sp_ud.set_defaults(func=users_delete)

    # EXTRA FIELDS
    sp_efl = sp.add_parser("extra-fields-list", help="Listar definiciones de extra fields")
    sp_efl.set_defaults(func=extra_fields_list)

    sp_vsef = sp.add_parser("visits-set-extrafields", help="Guardar valores de extrafields en visitas")
    sp_vsef.add_argument("--visits", nargs="*", type=int)
    sp_vsef.add_argument("--json-string", dest="json_string", help='String JSON, ej: {"ok":true}')
    sp_vsef.set_defaults(func=visits_set_extrafields)

    # SKILLS
    sp_sl = sp.add_parser("skills-list", help="Listar skills")
    sp_sl.set_defaults(func=skills_list)

    # ZONES
    sp_zl = sp.add_parser("zones-list", help="Listar zonas (si está habilitado)")
    sp_zl.set_defaults(func=zones_list)

    # FLEETS
    sp_fl = sp.add_parser("fleets-list", help="Listar flotas")
    sp_fl.set_defaults(func=fleets_list)

    # CHECKOUT
    sp_co = sp.add_parser("visits-checkout", help="Checkout de visita (firma base64 opcional)")
    sp_co.add_argument("--visit-id")
    sp_co.add_argument("--status", choices=["pending", "completed", "failed"], default="completed")
    sp_co.add_argument("--checkout-time", dest="checkout_time")
    sp_co.add_argument("--lat")
    sp_co.add_argument("--lon")
    sp_co.add_argument("--comment", default="Entrega OK")
    sp_co.add_argument("--signature-path", dest="signature_path", help="Ruta a imagen (png/jpg) para firma")
    sp_co.set_defaults(func=visits_checkout)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
