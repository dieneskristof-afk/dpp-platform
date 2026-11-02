from flask import Flask, render_template, request, redirect, url_for, session, send_from_directory, abort
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import uuid
import sqlite3
import os
import csv
import shutil
import json
import urllib.request
import urllib.error
import threading
import logging
import time
from pathlib import Path

app = Flask(__name__)
# A Central és a Factory azonos localhost címen is külön cookie-t használ.
app.config["SESSION_COOKIE_NAME"] = "dinexa_factory_session"
app.secret_key = os.environ.get("DINEXA_FACTORY_SECRET_KEY", "dpp-demo-secret-key")
# A futásidejű adatok a Render tartós lemezére kerülnek, ha
# DINEXA_FACTORY_DATA_DIR=/var/data/factory van beállítva.
# A forráskódban nincsenek gyártóspecifikus alapadatok.
FACTORY_DATA_DIR = Path(os.environ.get("DINEXA_FACTORY_DATA_DIR", str(Path(__file__).resolve().parent / ".factory_runtime")))
FACTORY_DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE = str(FACTORY_DATA_DIR / "dpp_factory.db")
QR_DATA_DIR = FACTORY_DATA_DIR / "qr"
QR_DATA_DIR.mkdir(parents=True, exist_ok=True)






def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS workstation_sync_state (
        workstation_code TEXT PRIMARY KEY,
        last_success TEXT NOT NULL
    )""")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS print_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            print_id TEXT UNIQUE NOT NULL,
            company TEXT NOT NULL,
            product TEXT NOT NULL,
            category TEXT,
            quantity INTEGER NOT NULL,
            employee_id TEXT,
            printed_at TEXT NOT NULL,
            status TEXT NOT NULL,
            product_id TEXT,
            synced INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reprint_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            print_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            employee_id TEXT,
            reprinted_at TEXT NOT NULL,
            synced INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            pin_hash TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id TEXT NOT NULL,
            product_id TEXT UNIQUE NOT NULL,
            category_code TEXT NOT NULL,
            category_name TEXT NOT NULL,
            product_name TEXT NOT NULL,
            dpp_id TEXT UNIQUE NOT NULL,
            sku TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            qr_file TEXT
        )
    """)

    cursor.execute("""CREATE TABLE IF NOT EXISTS synced_categories (
        id INTEGER NOT NULL, manufacturer_id INTEGER NOT NULL, name TEXT NOT NULL,
        parent_id INTEGER, PRIMARY KEY(id,manufacturer_id))""")
    cursor.execute("PRAGMA table_info(products)")
    existing_product_cols = {r[1] for r in cursor.fetchall()}
    for column, declaration in (("central_category_id", "INTEGER"), ("dpp_version", "INTEGER")):
        if column not in existing_product_cols:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {column} {declaration}")

    # Egyedi akkumulátor-azonosítók; az eseményazonosító csak a naplószinkronhoz kell.
    cursor.execute("""CREATE TABLE IF NOT EXISTS battery_serials (
        serial TEXT PRIMARY KEY,
        product_id TEXT NOT NULL,
        print_id TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    cursor.execute("""CREATE INDEX IF NOT EXISTS idx_battery_serials_print
        ON battery_serials(print_id)""")

    # print_log migráció
    cursor.execute("PRAGMA table_info(print_log)")
    columns = [column[1] for column in cursor.fetchall()]

    if "product_id" not in columns:
        cursor.execute("""
            ALTER TABLE print_log
            ADD COLUMN product_id TEXT
        """)

    if "synced" not in columns:
        cursor.execute("""
            ALTER TABLE print_log
            ADD COLUMN synced INTEGER NOT NULL DEFAULT 0
        """)

    # products migráció
    cursor.execute("PRAGMA table_info(products)")
    product_columns = [column[1] for column in cursor.fetchall()]

    if "qr_file" not in product_columns:
        cursor.execute("""
            ALTER TABLE products
            ADD COLUMN qr_file TEXT
        """)

    # users migráció
    cursor.execute("PRAGMA table_info(users)")
    user_columns = [column[1] for column in cursor.fetchall()]

    if "pin_hash" not in user_columns:
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN pin_hash TEXT
        """)

    conn.commit()
    conn.close()









def sync_data():
    """Legacy compatibility: never mark events synced without Central ACK."""
    _SYNC_WAKE.set()


def _upload_print_events(config):
    # Csak szerver-visszaigazolas utan jelolunk szinkronizaltnak.
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    try:
        prints = conn.execute('SELECT * FROM print_log WHERE synced=0 ORDER BY id LIMIT 100').fetchall()
        reprints = conn.execute('''SELECT r.*, p.product_id, p.product, p.category FROM reprint_log r
            LEFT JOIN print_log p ON p.print_id=r.print_id WHERE r.synced=0 ORDER BY r.id LIMIT 100''').fetchall()
        events = []
        for r in prints:
            events.append(dict(event_key=f"{config['workstation_code']}:print:{r['print_id']}", event_type='print',
                print_id=r['print_id'], product_id=r['product_id'], product_name=r['product'],
                category=r['category'], username=r['employee_id'], quantity=r['quantity'], occurred_at=r['printed_at']))
        for r in reprints:
            events.append(dict(event_key=f"{config['workstation_code']}:reprint:{r['id']}:{r['print_id']}", event_type='reprint',
                print_id=r['print_id'], product_id=r['product_id'], product_name=r['product'] or 'Ismeretlen termék',
                category=r['category'], username=r['employee_id'], quantity=r['quantity'], occurred_at=r['reprinted_at']))
        if not events:
            return 0
        result = _central_request(config, '/api/factory/v1/print-events', {'events':events}, authenticated=True)
        acknowledged = set(result.get('acknowledged', []))
        if not acknowledged.issubset({e['event_key'] for e in events}):
            raise ValueError('Hibás szerver-visszaigazolás')
        with conn:
            for r in prints:
                if f"{config['workstation_code']}:print:{r['print_id']}" in acknowledged:
                    conn.execute('UPDATE print_log SET synced=1 WHERE id=?', (r['id'],))
            for r in reprints:
                if f"{config['workstation_code']}:reprint:{r['id']}:{r['print_id']}" in acknowledged:
                    conn.execute('UPDATE reprint_log SET synced=1 WHERE id=?', (r['id'],))
        return len(acknowledged)
    finally:
        conn.close()


# Automatikus háttérszinkron: a dolgozó semmilyen gombot nem nyom.
# A napló a helyi SQLite-ban marad addig, amíg a Central vissza nem igazolja.
_SYNC_WAKE = threading.Event()
_SYNC_START_LOCK = threading.Lock()
_SYNC_STARTED = False
_DB_INITIALIZED = False
_DB_INIT_LOCK = threading.Lock()
_SYNC_INTERVAL = max(15, int(os.environ.get("DINEXA_SYNC_INTERVAL_SECONDS", "60")))
_SYNC_LOG = logging.getLogger("dinexa.factory.sync")


def _record_successful_sync(config):
    """Persist the latest successful catalog sync for this workstation."""
    code = config.get("workstation_code")
    if not code:
        return
    with sqlite3.connect(DATABASE, timeout=10) as db:
        db.execute("""INSERT INTO workstation_sync_state(workstation_code,last_success)
            VALUES(?,?) ON CONFLICT(workstation_code) DO UPDATE SET
            last_success=excluded.last_success""",
            (code, datetime.now().strftime("%Y.%m.%d. %H:%M:%S")))


def _last_successful_sync(config):
    code = config.get("workstation_code")
    if not code:
        return "—"
    with sqlite3.connect(DATABASE, timeout=10) as db:
        row = db.execute("SELECT last_success FROM workstation_sync_state WHERE workstation_code=?", (code,)).fetchone()
    return row[0] if row else "—"


def _acknowledge_local_dpp_versions(config):
    """A Central csak a már helyi SQLite-ba mentett verziókat igazolja vissza."""
    # A korábban letöltött, de még nem jóváhagyott régi helyi rekordokat
    # nem jelentjük jóváhagyottként; a helyi működésüket sem tiltjuk.
    catalog = _central_request(config, '/api/factory/v1/workstations/sync', authenticated=True)
    allowed = {(int(p["id"]), int(p["latest_dpp_version"])) for p in catalog.get("products", [])
               if p.get("latest_dpp_version") is not None}
    with sqlite3.connect(DATABASE) as db:
        products = [dict(product_id=int(r[0]), version_number=int(r[1]))
                    for r in db.execute("""SELECT product_id,dpp_version FROM products
                    WHERE company_id=? AND active=1 AND dpp_version IS NOT NULL""",
                    (config["manufacturer_code"],)).fetchall()
                    if (int(r[0]),int(r[1])) in allowed]
    result = _central_request(config, '/api/factory/v1/dpp-sync-ack',
                              {'products':products}, authenticated=True)
    if int(result.get('acknowledged', -1)) != len(products):
        raise ValueError('A Central nem igazolta vissza az összes DPP-t.')


def _automatic_sync_once():
    config = _factory_connection()
    if not config or not config.get("workstation_token"):
        return
    # A naplófeltöltés független a katalógusletöltéstől: egy QR-hiba nem
    # akadályozhatja meg a már elkészült nyomtatások továbbítását.
    try:
        uploaded_total = 0
        while True:
            uploaded = _upload_print_events(config)
            uploaded_total += uploaded
            if not uploaded:
                break
        if uploaded_total:
            _SYNC_LOG.warning("Central visszaigazolta a nyomtatási eseményeket: %d", uploaded_total)
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        _SYNC_LOG.warning("Naplószinkron később újrapróbálva: %s", exc)
    try:
        _sync_factory_users(config)
        FACTORY_CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        _record_successful_sync(config)
        # Az ACK külön hálózati művelet; hiba esetén a helyi katalógus érintetlen.
        try:
            _acknowledge_local_dpp_versions(config)
        except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
            _SYNC_LOG.warning("DPP-visszaigazolás később újrapróbálva: %s", exc)
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        _SYNC_LOG.warning("Katalógusszinkron később újrapróbálva: %s", exc)


def _automatic_sync_loop():
    while True:
        try:
            _automatic_sync_once()
        except Exception:
            _SYNC_LOG.exception("Váratlan háttérszinkron-hiba")
        _SYNC_WAKE.wait(timeout=_SYNC_INTERVAL)
        _SYNC_WAKE.clear()


def _start_automatic_sync():
    global _SYNC_STARTED, _DB_INITIALIZED
    # Gunicorn alatt a __main__ ág nem fut le: új, üres Render példányon
    # a helyi SQLite sémáját első kérés előtt létre kell hozni.
    with _DB_INIT_LOCK:
        if not _DB_INITIALIZED:
            init_db()
            _DB_INITIALIZED = True
    with _SYNC_START_LOCK:
        if _SYNC_STARTED:
            return
        _SYNC_STARTED = True
        threading.Thread(target=_automatic_sync_loop, name="dinexa-factory-sync", daemon=True).start()


@app.before_request
def _ensure_automatic_sync():
    # A WSGI-kiszolgálóban is induljon, ne csak a python app.py használatakor.
    _start_automatic_sync()
    if _manual_reactivation_requested():
        session.clear()
        if request.endpoint not in ("activate_workstation", "static"):
            return redirect(url_for("activate_workstation"))


@app.route("/factory/activation-status", methods=["GET"])
def factory_activation_status():
    """A böngésző a háttérszinkron által jelzett újraaktiválást ellenőrzi."""
    from flask import jsonify
    response = jsonify({"reactivation_required": (
        _manual_reactivation_requested() or _factory_connection() is None
    )})
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/sync", methods=["POST"])
def sync():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    config = _factory_connection()
    if not config:
        return redirect(url_for("activate_workstation"))
    try:
        user_count, product_count = _sync_factory_users(config)
        uploaded_count = _upload_print_events(config)
        FACTORY_CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        _record_successful_sync(config)
        _acknowledge_local_dpp_versions(config)
        session["sync_status"] = f"{user_count} felhasználó, {product_count} publikált DPP, {uploaded_count} naplóesemény szinkronizálva"
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
        session["sync_status"] = "Szinkronizáció sikertelen: " + str(exc)
    return redirect(url_for("index"))






@app.route("/sync-login", methods=["POST"])
def sync_login():

    _SYNC_WAKE.set()

    session["last_sync"] = "Automatikus szinkronizáció folyamatban"

    return redirect(url_for("login"))






    
def get_product(product_id):
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM products
        WHERE product_id = ?
        AND active = 1
    """, (product_id,))

    product = cursor.fetchone()

    conn.close()

    return product





@app.route("/product/<dpp_id>")
def public_product(dpp_id):

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM products
        WHERE dpp_id = ?
        AND active = 1
    """, (dpp_id,))

    product = cursor.fetchone()

    conn.close()

    if not product:
        return "A termék nem található.", 404

    return render_template(
        "product.html",
        product=product
    )
















# DINEXA Central kapcsolat: az aktiválás és a felhasználók szinkronja.
FACTORY_CONFIG = FACTORY_DATA_DIR / "factory_connection.json"
REACTIVATION_REQUIRED = FACTORY_CONFIG.with_name("factory_reactivation_required.flag")
# Korábbi verziók automatikusan is létrehoztak üres jelzőfájlt 401-es hibára.
# Csak a kifejezetten kézzel kért gyártóváltás legyen érvényes.
def _manual_reactivation_requested():
    try:
        return REACTIVATION_REQUIRED.read_text(encoding="utf-8").strip() == "manual"
    except OSError:
        return False

# Fejlesztéskor localhost; éles telepítésnél DINEXA_CENTRAL_URL környezeti változó.
CENTRAL_URL = os.environ.get("DINEXA_CENTRAL_URL", "http://127.0.0.1:5000").strip().rstrip("/")


def _factory_connection():
    if not FACTORY_CONFIG.exists():
        return None
    try:
        return json.loads(FACTORY_CONFIG.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def _central_request(config, endpoint, payload=None, authenticated=False):
    base = config["central_url"].rstrip("/")
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if authenticated:
        headers["Authorization"] = "Bearer " + config["workstation_token"]
    req = urllib.request.Request(base + endpoint, data=data, headers=headers,
                                 method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            raw = response.read(5_000_000)
            content_type = response.headers.get("Content-Type", "")
            try:
                result = json.loads(raw.decode("utf-8-sig"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise ValueError(
                    f"A Central nem JSON-választ adott ({endpoint}; HTTP {response.status}; "
                    f"Content-Type: {content_type}; válasz eleje: {raw[:140]!r})"
                ) from exc
            if not isinstance(result, dict):
                raise ValueError(f"A Central hibás JSON-struktúrát adott: {endpoint}")
            return result
    except urllib.error.HTTPError as exc:
        # A Central 401-es válasza nem törölheti a munkaállomás aktiválását.
        # Átmeneti token- vagy szerverhiba esetén az adatokat megőrizzük;
        # gyártóváltást kizárólag a bejelentkezési oldal manuális gombja kérhet.
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("error", "Szerverhiba")
        except (ValueError, UnicodeDecodeError):
            detail = "HTTP " + str(exc.code)
        raise ValueError(detail) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise ValueError("A DINEXA Central nem érhető el. Ellenőrizd a címet és a kapcsolatot.") from exc


def _sync_factory_users(config):
    data = _central_request(config, "/api/factory/v1/workstations/sync", authenticated=True)
    if int(data.get("manufacturer_id", -1)) != int(config["manufacturer_id"]):
        raise ValueError("A Central más gyártó adatait küldte. A szinkronizáció megszakadt.")
    users = data.get("users", [])
    categories = data.get("categories", [])
    products = data.get("products", [])
    if not isinstance(users, list) or not isinstance(categories, list) or not isinstance(products, list):
        raise ValueError("Hibás katalógusadatok érkeztek a Centraltól.")
    if isinstance(data.get("manufacturer_name"), str) and data["manufacturer_name"].strip():
        config["manufacturer_name"] = data["manufacturer_name"].strip()
    if isinstance(data.get("workstation_name"), str) and data["workstation_name"].strip():
        config["workstation_name"] = data["workstation_name"].strip()

    # Először a Central által generált, azonos terméktípushoz állandó QR-képek
    # letöltése; ha bármelyik hibás, a régi helyi katalógus változatlan marad.
    qr_dir = QR_DATA_DIR
    qr_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    published = [p for p in products if p.get("status") == "published" and p.get("latest_dpp_version") is not None]
    try:
        for p in published:
            pid = int(p["id"])
            filename = f"dinexa-central-dpp-{pid}.png"
            url = config["central_url"].rstrip("/") + f"/dpp/{pid}/qr.png"
            with urllib.request.urlopen(url, timeout=15) as response:
                content = response.read(2_000_001)
                if len(content) > 2_000_000 or not content.startswith(b"\x89PNG\r\n\x1a\n"):
                    raise ValueError("Érvénytelen QR-kép érkezett a Centraltól.")
            tmp = qr_dir / (filename + ".tmp")
            tmp.write_bytes(content)
            downloaded.append((tmp, qr_dir / filename))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        for tmp, _ in downloaded:
            tmp.unlink(missing_ok=True)
        raise ValueError("A DPP QR-kódok letöltése sikertelen: " + str(exc)) from exc

    conn = sqlite3.connect(DATABASE)
    try:
        conn.execute("BEGIN")
        conn.execute("UPDATE users SET active=0")
        for user in users:
            username = user["username"]
            conn.execute("""INSERT INTO users(employee_id,name,active,pin_hash)
                VALUES(?,?,?,NULL) ON CONFLICT(employee_id) DO UPDATE SET
                name=excluded.name,active=excluded.active,pin_hash=NULL""",
                (username, username, int(bool(user["is_active"]))))
        mid = int(config["manufacturer_id"])
        conn.execute("DELETE FROM synced_categories WHERE manufacturer_id=?", (mid,))
        for cat in categories:
            conn.execute("INSERT INTO synced_categories(id,manufacturer_id,name,parent_id) VALUES(?,?,?,?)",
                         (int(cat["id"]),mid,cat["name"],cat.get("parent_id")))
        company = config["manufacturer_code"]
        # Offline-first: a korábban szinkronizált termékek aktívak maradnak.
        # A Central csak jóváhagyott DPP-verziókat adhat hozzá/frissíthet.
        # A függőben lévő verzió nem írja felül a helyi, működő változatot.
        # A termékek kivezetését később külön, explicit visszavonási protokoll kezelheti.
        for p in published:
            pid = int(p["id"])
            cat_id = p.get("category_id")
            cat = next((c for c in categories if c["id"] == cat_id), None)
            cat_name = cat["name"] if cat else "Besorolatlan"
            conn.execute("""INSERT INTO products(company_id,product_id,category_code,category_name,
                product_name,dpp_id,sku,active,qr_file,central_category_id,dpp_version)
                VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET
                company_id=excluded.company_id,category_code=excluded.category_code,
                category_name=excluded.category_name,product_name=excluded.product_name,
                dpp_id=excluded.dpp_id,sku=excluded.sku,active=1,
                qr_file=excluded.qr_file,central_category_id=excluded.central_category_id,
                dpp_version=excluded.dpp_version""",
                (company,str(pid),str(cat_id) if cat_id is not None else "uncategorized",cat_name,
                 p["name"],str(pid),p.get("product_code"),1,
                 f"dinexa-central-dpp-{pid}.png",cat_id,int(p["latest_dpp_version"])))
        for tmp, target in downloaded:
            tmp.replace(target)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(users), len(published)


@app.route("/factory/qr/<path:filename>")
def factory_qr(filename):
    """A tartós lemezen tárolt, Centralból szinkronizált QR-képek kiszolgálása."""
    if "employee_id" not in session:
        return redirect(url_for("login"))
    return send_from_directory(QR_DATA_DIR, filename)


@app.route("/activate", methods=["GET", "POST"])
def activate_workstation():
    if _factory_connection() and not _manual_reactivation_requested():
        return redirect(url_for("login"))
    error = None
    if request.method == "POST":
        config = {
            "central_url": (_factory_connection() or {}).get("central_url", CENTRAL_URL),
            "manufacturer_code": request.form.get("manufacturer_code", "").strip(),
            "workstation_code": request.form.get("workstation_code", "").strip(),
        }
        activation_code = request.form.get("activation_code", "").strip()
        if not all((config["manufacturer_code"], config["workstation_code"], activation_code)):
            error = "Minden mezőt ki kell tölteni."
        else:
            try:
                result = _central_request(config, "/api/factory/v1/workstations/activate", {
                    "manufacturer_code": config["manufacturer_code"],
                    "workstation_code": config["workstation_code"],
                    "activation_code": activation_code,
                })
                config["manufacturer_id"] = int(result["manufacturer_id"])
                config["manufacturer_name"] = result.get("manufacturer_name") or result.get("company_name") or ""
                config["workstation_name"] = result.get("workstation_name") or ""
                config["workstation_token"] = result["workstation_token"]
                _sync_factory_users(config)
                FACTORY_CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
                try:
                    os.chmod(FACTORY_CONFIG, 0o600)
                except OSError:
                    pass
                REACTIVATION_REQUIRED.unlink(missing_ok=True)
                session.clear()
                return redirect(url_for("login"))
            except (ValueError, KeyError, TypeError) as exc:
                error = str(exc)
    return render_template("activate.html", error=error)


@app.route("/central-sync", methods=["POST"])
def central_sync():
    config = _factory_connection()
    if not config:
        return redirect(url_for("activate_workstation"))
    try:
        count, product_count = _sync_factory_users(config)
        session["sync_message"] = f"{count} felhasználó, {product_count} DPP szinkronizálva."
    except ValueError as exc:
        session["sync_message"] = "Szinkronizáció sikertelen: " + str(exc)
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    config = _factory_connection()
    if not config:
        return redirect(url_for("activate_workstation"))
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        try:
            # Online hitelesítés: a jelszó nem kerül a Factory adatbázisába.
            result = _central_request(config, "/api/factory/v1/login", {
                "manufacturer_id": config["manufacturer_id"],
                "username": username, "password": password,
            })
            identity = result["user"]
            if int(identity["manufacturer_id"]) != int(config["manufacturer_id"]):
                raise ValueError("Eltérő gyártóazonosító a bejelentkezésnél.")
            session.clear()
            session["employee_id"] = identity["username"]
            session["employee_name"] = identity["username"]
            session["factory_role"] = identity["role"]
            return redirect(url_for("index"))
        except (ValueError, KeyError, TypeError) as exc:
            error = str(exc)
    message = session.pop("sync_message", None)
    return render_template("login.html", error=error, sync_message=message)

@app.route("/switch-workstation", methods=["POST"])
def switch_workstation():
    """Ideiglenes gyártóváltás a belépési oldalról; a naplók megmaradnak."""
    # A még nem szinkronizált események nem kerülhetnek másik gyártóhoz.
    with sqlite3.connect(DATABASE) as conn:
        pending_prints = conn.execute("SELECT COUNT(*) FROM print_log WHERE synced=0").fetchone()[0]
        pending_reprints = conn.execute("SELECT COUNT(*) FROM reprint_log WHERE synced=0").fetchone()[0]
    if pending_prints or pending_reprints:
        session["sync_message"] = (
            "Gyártóváltás nem lehetséges: "
            f"{pending_prints + pending_reprints} nyomtatási esemény még nincs szinkronizálva."
        )
        return redirect(url_for("login"))
    REACTIVATION_REQUIRED.write_text("manual", encoding="utf-8")
    session.clear()
    return redirect(url_for("activate_workstation"))


@app.route("/logout")




def logout():

    session.clear()

    return redirect(url_for("login"))

@app.route("/")






def index():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    config = _factory_connection() or {}
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    manufacturer_id = int(config.get("manufacturer_id", 0))
    categories = [dict(r) for r in cursor.execute("""SELECT id,name,parent_id
        FROM synced_categories WHERE manufacturer_id=? ORDER BY name COLLATE NOCASE,id""", (manufacturer_id,))]
    products = [dict(r) for r in cursor.execute("""SELECT product_id,product_name,sku,
        central_category_id,dpp_version,qr_file FROM products
        WHERE company_id=? AND active=1 AND dpp_version IS NOT NULL
        ORDER BY product_name""", (config.get("manufacturer_code", ""),))]
    products_by_category = {}
    for product in products:
        code = str(product["central_category_id"] or "uncategorized")
        products_by_category.setdefault(code, []).append({"product_id": product["product_id"],
            "product_name": product["product_name"]})

    conn.close()

    return render_template(
        "index.html",
        employee_id=session["employee_id"],
        employee_name=session["employee_name"],
        products=products,
        products_by_category=products_by_category,
        catalog_categories=categories,
        catalog_products=products,
        manufacturer_name=config.get("manufacturer_name") or "Nincs gyártónév szinkronizálva",
        manufacturer_code=config.get("manufacturer_code", "—"),
        workstation_name=config.get("workstation_name", ""),
        workstation_code=config.get("workstation_code", "—"),
        sync_status=session.pop("sync_status", None),
        last_sync=_last_successful_sync(config),
    )

@app.route("/print")






def print_page():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    product_id = request.args.get("product_id", "")

    product = get_product(product_id)

    if not product:
        return "Ismeretlen termék.", 404

    now = datetime.now()

    return render_template(
        "print.html",
        product=product,
        date=now.strftime("%Y.%m.%d."),
        time=now.strftime("%H:%M")
    )


@app.route("/confirm", methods=["POST"])






def confirm():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    product_id = request.form.get("product_id", "")
    quantity = request.form.get("quantity", "1")

    product = get_product(product_id)

    if not product:
        return "Ismeretlen termék.", 404

    now = datetime.now()

    return render_template(
        "confirm.html",
        product=product,
        quantity=quantity,
        date=now.strftime("%Y.%m.%d."),
        time=now.strftime("%H:%M:%S")
    )









def _is_battery_product(product):
    """Az akkumulátorkategóriához tartozó termékek kapnak BAT-F sorozatszámot."""
    values = [str(product[key] or "").casefold()
              for key in ("product_name", "category_name", "sku", "product_id")]
    return any("akkumul" in v or "battery" in v or "bat-home" in v
               or "home 5" in v for v in values)


def _allocate_battery_serials(conn, product_id, print_id, quantity, now):
    """Globálisan növekvő BAT-F sorszám ezen a Factory-adatbázison belül.

    A BEGIN IMMEDIATE tranzakció megakadályozza a párhuzamos kiosztási ütközést.
    A régi BAT-HOME-5-F sorozatok számait is figyelembe vesszük.
    """
    prefix = "BAT-F-"
    previous_serials = conn.execute("SELECT serial FROM battery_serials").fetchall()
    max_number = 0
    for (old_serial,) in previous_serials:
        if old_serial.startswith(("BAT-F-", "BAT-HOME-5-F-")):
            suffix = old_serial.rsplit("-", 1)[-1]
            if suffix.isdigit():
                max_number = max(max_number, int(suffix))
    next_number = max_number + 1
    if next_number + quantity - 1 > 9999999:
        raise ValueError("A hétjegyű Factory sorozatszám-tartomány betelt.")
    serials = [f"{prefix}{number:07d}" for number in range(next_number, next_number + quantity)]
    conn.executemany(
        "INSERT INTO battery_serials(serial,product_id,print_id,created_at) VALUES(?,?,?,?)",
        [(serial, product_id, print_id, now) for serial in serials])
    return serials


@app.route("/execute-print", methods=["POST"])
def execute_print():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    product_id = request.form.get("product_id", "")
    quantity = request.form.get("quantity", "1")

    product = get_product(product_id)

    if not product:
        return "Ismeretlen termék.", 404

    qr_filename = product["qr_file"]

    if not qr_filename:
        return "Ehhez a termékhez nincs QR-kód rendelve.", 404

    qr_path = QR_DATA_DIR / qr_filename

    if not os.path.exists(qr_path):
        return "A termék QR-kódja nincs letöltve erre az eszközre.", 404

    now = datetime.now()

    # Belső eseménykulcs a Central/Portal naplózásához; nem kerül a címkére.
    print_id = "EVT-" + uuid.uuid4().hex.upper()

    try:
        quantity = int(quantity)
    except (TypeError, ValueError):
        return "Hibás darabszám.", 400
    if quantity < 1 or quantity > 1000:
        return "A darabszám 1 és 1000 között lehet.", 400

    conn = sqlite3.connect(DATABASE, timeout=15)
    cursor = conn.cursor()
    try:
        conn.execute("BEGIN IMMEDIATE")
        serials = (_allocate_battery_serials(conn, product["product_id"], print_id,
                                             quantity, now.strftime("%Y-%m-%d %H:%M:%S"))
                   if _is_battery_product(product) else [])
        cursor.execute("""
        INSERT INTO print_log (
            print_id,
            product_id,
            company,
            product,
            category,
            quantity,
            employee_id,
            printed_at,
            status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        print_id,
        product["product_id"],
        product["company_id"],
        product["product_name"],
        product["category_name"],
        quantity,
        session["employee_id"],
        now.strftime("%Y-%m-%d %H:%M:%S"),
        "sikeres"
    ))
        conn.commit()
    except (sqlite3.Error, ValueError) as exc:
        conn.rollback()
        return f"A sorozatszám kiosztása nem sikerült: {exc}", 409
    finally:
        conn.close()
    _SYNC_WAKE.set()

    return render_template(
        "label.html",
        qr_filename=qr_filename,
        print_id=print_id,
        quantity=quantity,
        serials=serials
    )






@app.route("/reprint/<print_id>", methods=["GET", "POST"])
def reprint(print_id):

    if "employee_id" not in session:
        return redirect(url_for("login"))

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            print_log.*,
            products.product_name AS current_product_name,
            products.qr_file AS qr_file
        FROM print_log
        LEFT JOIN products
            ON print_log.product_id = products.product_id
        WHERE print_log.print_id = ?
    """, (print_id,))

    original_print = cursor.fetchone()
    conn.close()

    if not original_print:
        return "Ismeretlen PRN azonosító.", 404

    if request.method == "POST":

        quantity = request.form.get("quantity", "1")

        try:
            quantity = int(quantity)
        except ValueError:
            return "Hibás darabszám.", 400

        if quantity < 1:
            return "A darabszám legalább 1 legyen.", 400

        qr_filename = original_print["qr_file"]

        if not qr_filename:
            return "A termékhez nincs QR-kód rendelve.", 404

        qr_path = QR_DATA_DIR / qr_filename

        if not os.path.exists(qr_path):
            return "A termék QR-kódja nincs letöltve erre az eszközre.", 404

        with sqlite3.connect(DATABASE) as serial_conn:
            existing_serials = [row[0] for row in serial_conn.execute(
                "SELECT serial FROM battery_serials WHERE print_id=? ORDER BY serial",
                (original_print["print_id"],)).fetchall()]
        if not existing_serials and str(original_print["category"] or "").casefold().find("akkumul") >= 0:
            return "A régi akkumulátornyomtatáshoz nincs egyedi sorozatszám; nem nyomtatható újra.", 409
        if existing_serials and quantity > len(existing_serials):
            return "Az eredeti sorozatszámok számánál több címke nem nyomtatható újra.", 400

        # Újranyomtatás naplózása
        now = datetime.now()

        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO reprint_log (
                print_id,
                quantity,
                employee_id,
                reprinted_at,
                synced
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            original_print["print_id"],
            quantity,
            session["employee_id"],
            now.strftime("%Y-%m-%d %H:%M:%S"),
            0
        ))

        conn.commit()
        conn.close()
        _SYNC_WAKE.set()

        # Akkumulátor esetén kizárólag az eredeti sorozatszámok újranyomtatása.
        serials = existing_serials[:quantity]
        return render_template(
            "label.html",
            qr_filename=qr_filename,
            print_id=original_print["print_id"],
            quantity=quantity,
            serials=serials
        )

    return render_template(
        "reprint.html",
        original_print=original_print
    )








@app.route("/log")
def print_log():
    if "employee_id" not in session:
        return redirect(url_for("login"))

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            print_log.*,
            products.product_name AS current_product_name,
            users.name AS employee_name
        FROM print_log
        LEFT JOIN products
            ON print_log.product_id = products.product_id
        LEFT JOIN users
            ON print_log.employee_id = users.employee_id
        ORDER BY print_log.id DESC
    """)

    logs = cursor.fetchall()

    conn.close()

    return render_template(
        "log.html",
        logs=logs
    )







@app.route("/search", methods=["GET", "POST"])
def search_print():
    if "employee_id" not in session:
        return redirect(url_for("login"))

    result = None
    searched = False

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # PRN keresés
    if request.method == "POST":
        print_id = request.form.get("print_id", "").strip().upper()
    else:
        print_id = request.args.get("print_id", "").strip().upper()

    if print_id:
            cursor.execute("""
                SELECT
                    print_log.*,
                    users.name AS employee_name,
                    products.product_name AS current_product_name,
                    products.category_name AS current_category_name
                FROM print_log
                LEFT JOIN users
                    ON print_log.employee_id = users.employee_id
                LEFT JOIN products
                    ON print_log.product_id = products.product_id
                WHERE print_log.print_id = ?
            """, (print_id,))

            result = cursor.fetchone()
            searched = True

    # Utolsó 20 nyomtatási esemény
    cursor.execute("""
        SELECT
            print_log.*,
            users.name AS employee_name,
            products.product_name AS current_product_name
        FROM print_log
        LEFT JOIN users
            ON print_log.employee_id = users.employee_id
        LEFT JOIN products
            ON print_log.product_id = products.product_id
        ORDER BY print_log.id DESC
        LIMIT 20
    """)

    history = cursor.fetchall()

    conn.close()

    return render_template(
        "search.html",
        result=result,
        searched=searched,
        history=history
    )


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5001)