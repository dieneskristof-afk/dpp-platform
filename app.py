from flask import Flask, render_template, request, redirect, url_for, session
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import uuid
import sqlite3
import os
import csv
import shutil

app = Flask(__name__)
app.secret_key = "dpp-demo-secret-key"
DATABASE = "dpp_factory.db"






def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

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
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Termékek szinkronizálása
    with open("products.csv", "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            cursor.execute("""
                INSERT INTO products (
                    company_id,
                    product_id,
                    category_code,
                    category_name,
                    product_name,
                    dpp_id,
                    qr_file,
                    sku,
                    active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    company_id = excluded.company_id,
                    category_code = excluded.category_code,
                    category_name = excluded.category_name,
                    product_name = excluded.product_name,
                    dpp_id = excluded.dpp_id,
                    qr_file = excluded.qr_file,
                    sku = excluded.sku,
                    active = excluded.active
            """, (
                row["company_id"],
                row["product_id"],
                row["category_code"],
                row["category_name"],
                row["product_name"],
                row["dpp_id"],
                row["qr_file"],
                row["sku"] or None,
                int(row["active"])
            ))

            # QR-kód szinkronizálása
            qr_filename = row["qr_file"].strip()

            if qr_filename:
                source_qr = os.path.join("central_qr", qr_filename)
                target_qr = os.path.join("static", "qr", qr_filename)

                if os.path.exists(source_qr):
                    shutil.copy2(source_qr, target_qr)

    # Felhasználók szinkronizálása
    with open("users.csv", "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            pin_hash = generate_password_hash(row["pin"])

            cursor.execute("""
                INSERT INTO users (
                    employee_id,
                    name,
                    active,
                    pin_hash
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(employee_id) DO UPDATE SET
                    name = excluded.name,
                    active = excluded.active,
                    pin_hash = excluded.pin_hash
            """, (
                row["employee_id"],
                row["name"],
                int(row["active"]),
                pin_hash
            ))

    # -----------------------------------
    # EREDETI NYOMTATÁSOK FELTÖLTÉSE
    # -----------------------------------

    cursor.execute("""
        SELECT *
        FROM print_log
        WHERE synced = 0
        ORDER BY id ASC
    """)

    unsynced_logs = cursor.fetchall()

    if unsynced_logs:
        file_exists = (
            os.path.exists("central_print_log.csv")
            and os.path.getsize("central_print_log.csv") > 0
        )

        with open(
            "central_print_log.csv",
            "a",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            fieldnames = [
                "print_id",
                "company",
                "product_id",
                "product",
                "category",
                "quantity",
                "employee_id",
                "printed_at",
                "status"
            ]

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            if not file_exists:
                writer.writeheader()

            for log in unsynced_logs:
                writer.writerow({
                    "print_id": log["print_id"],
                    "company": log["company"],
                    "product_id": log["product_id"],
                    "product": log["product"],
                    "category": log["category"],
                    "quantity": log["quantity"],
                    "employee_id": log["employee_id"],
                    "printed_at": log["printed_at"],
                    "status": log["status"]
                })

                cursor.execute("""
                    UPDATE print_log
                    SET synced = 1
                    WHERE id = ?
                """, (log["id"],))

    # -----------------------------------
    # ÚJRANYOMTATÁSOK FELTÖLTÉSE
    # -----------------------------------

    cursor.execute("""
        SELECT *
        FROM reprint_log
        WHERE synced = 0
        ORDER BY id ASC
    """)

    unsynced_reprints = cursor.fetchall()

    if unsynced_reprints:
        reprint_file_exists = (
            os.path.exists("central_reprint_log.csv")
            and os.path.getsize("central_reprint_log.csv") > 0
        )

        with open(
            "central_reprint_log.csv",
            "a",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            fieldnames = [
                "print_id",
                "quantity",
                "employee_id",
                "reprinted_at"
            ]

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            if not reprint_file_exists:
                writer.writeheader()

            for log in unsynced_reprints:
                writer.writerow({
                    "print_id": log["print_id"],
                    "quantity": log["quantity"],
                    "employee_id": log["employee_id"],
                    "reprinted_at": log["reprinted_at"]
                })

                cursor.execute("""
                    UPDATE reprint_log
                    SET synced = 1
                    WHERE id = ?
                """, (log["id"],))

    conn.commit()
    conn.close()








@app.route("/sync", methods=["POST"])
def sync():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    sync_data()

    session["last_sync"] = datetime.now().strftime("%Y.%m.%d. %H:%M")

    return redirect(url_for("index"))






@app.route("/sync-login", methods=["POST"])
def sync_login():

    sync_data()

    session["last_sync"] = datetime.now().strftime("%Y.%m.%d. %H:%M")

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
















@app.route("/login", methods=["GET", "POST"])
def login():

    error = None

    if request.method == "POST":

        employee_id = request.form.get(
            "employee_id", ""
        ).strip().upper()

        pin = request.form.get(
            "pin", ""
        ).strip()

        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT *
            FROM users
            WHERE employee_id = ?
            AND active = 1
        """, (employee_id,))

        user = cursor.fetchone()

        conn.close()

        if (
            user
            and user["pin_hash"]
            and check_password_hash(user["pin_hash"], pin)
        ):
            session["employee_id"] = user["employee_id"]
            session["employee_name"] = user["name"]

            return redirect(url_for("index"))

        error = "Hibás dolgozói azonosító vagy PIN-kód."

    return render_template(
        "login.html",
        error=error
    )

@app.route("/logout")




def logout():

    session.clear()

    return redirect(url_for("login"))

@app.route("/")






def index():

    if "employee_id" not in session:
        return redirect(url_for("login"))

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            product_id,
            category_code,
            category_name,
            product_name,
            dpp_id,
            sku
        FROM products
        WHERE company_id = ?
        AND active = 1
        ORDER BY category_name, product_name
    """, ("HU-DEMO-001",))

    products = cursor.fetchall()
    products_by_category = {}

    for product in products:
        category_code = product["category_code"].lower()

        if category_code not in products_by_category:
            products_by_category[category_code] = []

        products_by_category[category_code].append({
            "product_id": product["product_id"],
            "product_name": product["product_name"]
        })

    conn.close()

    return render_template(
        "index.html",
        employee_id=session["employee_id"],
        employee_name=session["employee_name"],
        products=products,
        products_by_category=products_by_category
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

    qr_path = os.path.join(
        app.static_folder,
        "qr",
        qr_filename
    )

    if not os.path.exists(qr_path):
        return "A termék QR-kódja nincs letöltve erre az eszközre.", 404

    now = datetime.now()

    print_id = (
        "PRN-"
        + now.strftime("%Y%m%d-%H%M%S")
        + "-"
        + uuid.uuid4().hex[:4].upper()
    )

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

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
        int(quantity),
        session["employee_id"],
        now.strftime("%Y-%m-%d %H:%M:%S"),
        "sikeres"
    ))

    conn.commit()
    conn.close()

    return render_template(
        "label.html",
        qr_filename=qr_filename,
        print_id=print_id,
        quantity=int(quantity)
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

        qr_path = os.path.join(
            app.static_folder,
            "qr",
            qr_filename
        )

        if not os.path.exists(qr_path):
            return "A termék QR-kódja nincs letöltve erre az eszközre.", 404

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

        return render_template(
            "label.html",
            qr_filename=qr_filename,
            print_id=original_print["print_id"],
            quantity=quantity
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
    app.run(debug=True)