from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from flask import Flask, abort, g, redirect, render_template, request, send_from_directory, url_for
from werkzeug.utils import secure_filename


BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = Path(
    os.getenv("APP_STORAGE_DIR")
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    or (BASE_DIR / "data")
)
DATABASE = Path(os.getenv("APP_DATABASE_PATH") or (STORAGE_DIR / "app.db"))
UPLOAD_DIR = Path(os.getenv("APP_UPLOAD_DIR") or (STORAGE_DIR / "uploads" / "jobs"))
PRODUCT_UPLOAD_DIR = Path(
    os.getenv("APP_PRODUCT_UPLOAD_DIR") or (STORAGE_DIR / "uploads" / "products")
)

app = Flask(__name__)
DATABASE.parent.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
PRODUCT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

JOB_STATUSES = [
    "Orcamento",
    "Aguardando aprovacao",
    "Aprovado",
    "Producao",
    "Pos-processo",
    "Pronto para entrega",
    "Entregue",
    "Cancelado",
]

MOVEMENT_TYPES = [
    "Entrada",
    "Ajuste positivo",
    "Ajuste negativo",
    "Consumo manual",
    "Perda",
]

PRINTER_STATUSES = [
    "Operando",
    "Em manutenção",
    "Parada",
    "Reservada",
]


def normalize_upper_text(value: str | None) -> str:
    return str(value or "").strip().upper()


def parse_loose_float(value: Any, default: float = 0.0) -> float:
    raw = str(value or "").strip()
    if not raw or raw.startswith("__"):
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def material_order_clause(prefix: str = "") -> str:
    return (
        f"{prefix}color COLLATE NOCASE ASC, "
        f"{prefix}material_type COLLATE NOCASE ASC, "
        f"COALESCE(NULLIF(TRIM({prefix}line_series), ''), NULLIF(TRIM({prefix}name), ''), '') COLLATE NOCASE ASC, "
        f"{prefix}manufacturer_name COLLATE NOCASE ASC, "
        f"{prefix}sku ASC, "
        f"{prefix}id ASC"
    )


def normalize_existing_customer_data(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT
            id,
            name,
            document,
            phone,
            email,
            customer_type,
            postal_code,
            street,
            address_number,
            address_complement,
            neighborhood,
            city,
            state,
            lead_source,
            segment,
            notes
        FROM customers
        """
    ).fetchall()
    for row in rows:
        db.execute(
            """
            UPDATE customers
            SET
                name = ?,
                document = ?,
                phone = ?,
                email = ?,
                customer_type = ?,
                postal_code = ?,
                street = ?,
                address_number = ?,
                address_complement = ?,
                neighborhood = ?,
                city = ?,
                state = ?,
                lead_source = ?,
                segment = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                normalize_upper_text(row["name"]),
                normalize_upper_text(row["document"]),
                str(row["phone"] or "").strip(),
                str(row["email"] or "").strip().lower(),
                normalize_upper_text(row["customer_type"]),
                str(row["postal_code"] or "").strip(),
                normalize_upper_text(row["street"]),
                normalize_upper_text(row["address_number"]),
                normalize_upper_text(row["address_complement"]),
                normalize_upper_text(row["neighborhood"]),
                normalize_upper_text(row["city"]),
                normalize_upper_text(row["state"]),
                normalize_upper_text(row["lead_source"]),
                normalize_upper_text(row["segment"]),
                normalize_upper_text(row["notes"]),
                row["id"],
            ),
        )


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_: Any) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def ensure_column(
    db: sqlite3.Connection, table_name: str, column_name: str, definition: str
) -> None:
    columns = {
        row["name"]
        for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in columns:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def init_db() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            document TEXT,
            phone TEXT,
            email TEXT,
            customer_type TEXT,
            postal_code TEXT,
            street TEXT,
            address_number TEXT,
            address_complement TEXT,
            neighborhood TEXT,
            city TEXT,
            state TEXT,
            lead_source TEXT,
            segment TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_name TEXT,
            phone TEXT,
            email TEXT,
            supplier_link TEXT,
            lead_time_days INTEGER NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS representatives (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            commission_percent REAL NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS partner_stores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city TEXT,
            contact_name TEXT,
            phone TEXT,
            instagram TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS payment_terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS sales_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS printers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            brand TEXT,
            model TEXT,
            serial_number TEXT,
            technology TEXT,
            nozzle_size REAL,
            build_volume TEXT,
            location TEXT,
            status TEXT,
            purchase_date TEXT,
            last_maintenance_date TEXT,
            next_maintenance_date TEXT,
            hourly_cost REAL NOT NULL DEFAULT 0,
            energy_watts REAL NOT NULL DEFAULT 0,
            purchase_value REAL NOT NULL DEFAULT 0,
            useful_life_hours REAL NOT NULL DEFAULT 0,
            has_ams INTEGER NOT NULL DEFAULT 0,
            ams_model TEXT,
            kwh_cost REAL NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS filament_dryers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT NOT NULL,
            model TEXT NOT NULL,
            dryer_type TEXT,
            power_watts REAL NOT NULL DEFAULT 0,
            useful_life_hours REAL NOT NULL DEFAULT 0,
            price REAL NOT NULL DEFAULT 0,
            kwh_cost REAL NOT NULL DEFAULT 0,
            hourly_cost REAL NOT NULL DEFAULT 0,
            is_default INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS operational_cost_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            monthly_fixed_cost REAL NOT NULL DEFAULT 0,
            productive_hours_per_month REAL NOT NULL DEFAULT 0,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS components (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            component_type TEXT,
            sku TEXT,
            manufacturer_name TEXT,
            part_number TEXT,
            compatible_with TEXT,
            location TEXT,
            unit_cost REAL NOT NULL DEFAULT 0,
            product_cost REAL NOT NULL DEFAULT 0,
            shipping_cost REAL NOT NULL DEFAULT 0,
            store_discount REAL NOT NULL DEFAULT 0,
            coupon_discount REAL NOT NULL DEFAULT 0,
            payment_discount REAL NOT NULL DEFAULT 0,
            real_total_cost REAL NOT NULL DEFAULT 0,
            unit_measure TEXT,
            stock_quantity REAL NOT NULL DEFAULT 0,
            minimum_quantity REAL NOT NULL DEFAULT 0,
            purchase_link TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            material_type TEXT NOT NULL,
            line_series TEXT,
            color TEXT NOT NULL,
            color_hex TEXT,
            lot_number TEXT,
            stock_grams REAL NOT NULL DEFAULT 0,
            cost_per_kg REAL NOT NULL DEFAULT 0,
            supplier_id INTEGER,
            sku TEXT,
            manufacturer_name TEXT,
            location TEXT,
            minimum_stock_grams REAL NOT NULL DEFAULT 250,
            purchase_link TEXT,
            product_cost REAL NOT NULL DEFAULT 0,
            shipping_cost REAL NOT NULL DEFAULT 0,
            store_discount REAL NOT NULL DEFAULT 0,
            coupon_discount REAL NOT NULL DEFAULT 0,
            payment_discount REAL NOT NULL DEFAULT 0,
            real_total_cost REAL NOT NULL DEFAULT 0,
            nozzle_temperature_c REAL NOT NULL DEFAULT 0,
            bed_temperature_c REAL NOT NULL DEFAULT 0,
            fan_speed_percent REAL NOT NULL DEFAULT 0,
            fan_speed_min_percent REAL NOT NULL DEFAULT 0,
            fan_speed_max_percent REAL NOT NULL DEFAULT 0,
            flow_percent REAL NOT NULL DEFAULT 0,
            flow_test_1_percent REAL NOT NULL DEFAULT 0,
            flow_test_2_percent REAL NOT NULL DEFAULT 0,
            retraction_distance_mm REAL NOT NULL DEFAULT 0,
            retraction_speed_mm_s REAL NOT NULL DEFAULT 0,
            pressure_advance REAL NOT NULL DEFAULT 0,
            print_speed_mm_s REAL NOT NULL DEFAULT 0,
            xy_compensation_mm REAL NOT NULL DEFAULT 0,
            humidity_percent REAL NOT NULL DEFAULT 0,
            drying_required INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id)
        );

        CREATE TABLE IF NOT EXISTS material_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT NOT NULL,
            item_name TEXT NOT NULL,
            status TEXT NOT NULL,
            material_id INTEGER NOT NULL,
            weight_grams REAL NOT NULL,
            print_hours REAL NOT NULL,
            energy_cost_per_hour REAL NOT NULL,
            operating_cost_per_hour REAL NOT NULL,
            extra_cost REAL NOT NULL DEFAULT 0,
            margin_percent REAL NOT NULL,
            total_cost REAL NOT NULL,
            suggested_price REAL NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            customer_id INTEGER,
            representative_id INTEGER,
            partner_store_id INTEGER,
            due_date TEXT,
            quantity INTEGER NOT NULL DEFAULT 1,
            sale_channel TEXT,
            FOREIGN KEY(material_id) REFERENCES materials(id),
            FOREIGN KEY(customer_id) REFERENCES customers(id),
            FOREIGN KEY(representative_id) REFERENCES representatives(id),
            FOREIGN KEY(partner_store_id) REFERENCES partner_stores(id)
        );

        CREATE TABLE IF NOT EXISTS job_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            weight_grams REAL NOT NULL DEFAULT 0,
            print_hours REAL NOT NULL DEFAULT 0,
            printer_id INTEGER,
            energy_cost_per_hour REAL NOT NULL DEFAULT 0,
            operating_cost_per_hour REAL NOT NULL DEFAULT 0,
            filament_dryer_id INTEGER,
            dryer_hours REAL NOT NULL DEFAULT 0,
            dryer_cost_per_hour REAL NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(material_id) REFERENCES materials(id),
            FOREIGN KEY(printer_id) REFERENCES printers(id),
            FOREIGN KEY(filament_dryer_id) REFERENCES filament_dryers(id)
        );

        CREATE TABLE IF NOT EXISTS job_components (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            component_id INTEGER NOT NULL,
            quantity REAL NOT NULL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(component_id) REFERENCES components(id)
        );

        CREATE TABLE IF NOT EXISTS job_services (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            service_name TEXT NOT NULL,
            category TEXT,
            quantity REAL NOT NULL DEFAULT 1,
            hours REAL NOT NULL DEFAULT 0,
            unit_price REAL NOT NULL DEFAULT 0,
            addition_value REAL NOT NULL DEFAULT 0,
            discount_value REAL NOT NULL DEFAULT 0,
            total_price REAL NOT NULL DEFAULT 0,
            show_to_customer INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS job_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            original_name TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS inventory_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            material_id INTEGER NOT NULL,
            movement_type TEXT NOT NULL,
            quantity_grams REAL NOT NULL,
            unit_cost_per_kg REAL NOT NULL DEFAULT 0,
            related_job_id INTEGER,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(material_id) REFERENCES materials(id),
            FOREIGN KEY(related_job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS commercial_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_group_id TEXT,
            entry_date TEXT,
            invoice_date TEXT,
            order_number TEXT,
            invoice_number TEXT,
            document_number TEXT,
            supplier_id INTEGER,
            item_kind TEXT NOT NULL,
            item_type TEXT,
            material_id INTEGER,
            component_id INTEGER,
            item_code TEXT,
            brand_name TEXT,
            line_description TEXT,
            color_name TEXT,
            quantity REAL NOT NULL DEFAULT 0,
            amount REAL NOT NULL DEFAULT 0,
            freight REAL NOT NULL DEFAULT 0,
            tax REAL NOT NULL DEFAULT 0,
            discount REAL NOT NULL DEFAULT 0,
            total_amount REAL NOT NULL DEFAULT 0,
            unit_cost REAL NOT NULL DEFAULT 0,
            site TEXT,
            product_name TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id),
            FOREIGN KEY(material_id) REFERENCES materials(id),
            FOREIGN KEY(component_id) REFERENCES components(id)
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT,
            name TEXT NOT NULL,
            category TEXT,
            description TEXT,
            material_id INTEGER,
            weight_grams REAL NOT NULL DEFAULT 0,
            print_hours REAL NOT NULL DEFAULT 0,
            energy_cost_per_hour REAL NOT NULL DEFAULT 0,
            operating_cost_per_hour REAL NOT NULL DEFAULT 0,
            labor_hours REAL NOT NULL DEFAULT 0,
            labor_hourly_rate REAL NOT NULL DEFAULT 0,
            design_hours REAL NOT NULL DEFAULT 0,
            design_hourly_rate REAL NOT NULL DEFAULT 0,
            extra_cost REAL NOT NULL DEFAULT 0,
            margin_percent REAL NOT NULL DEFAULT 0,
            unit_cost REAL NOT NULL DEFAULT 0,
            sale_price REAL NOT NULL DEFAULT 0,
            stock_quantity REAL NOT NULL DEFAULT 0,
            minimum_quantity REAL NOT NULL DEFAULT 0,
            sale_channel TEXT,
            status TEXT NOT NULL DEFAULT 'Ativo',
            model_link TEXT,
            photo_path TEXT,
            photo_original_name TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(material_id) REFERENCES materials(id)
        );
        """
    )

    ensure_column(db, "materials", "supplier_id", "INTEGER")
    ensure_column(db, "materials", "sku", "TEXT")
    ensure_column(db, "materials", "line_series", "TEXT")
    ensure_column(db, "materials", "manufacturer_name", "TEXT")
    ensure_column(db, "materials", "color_hex", "TEXT")
    ensure_column(db, "materials", "lot_number", "TEXT")
    ensure_column(db, "materials", "location", "TEXT")
    ensure_column(db, "materials", "minimum_stock_grams", "REAL NOT NULL DEFAULT 250")
    ensure_column(db, "materials", "purchase_link", "TEXT")
    ensure_column(db, "materials", "product_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "shipping_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "store_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "coupon_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "payment_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "real_total_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "nozzle_temperature_c", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "bed_temperature_c", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "fan_speed_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "fan_speed_min_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "fan_speed_max_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "flow_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "flow_test_1_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "flow_test_2_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(
        db, "materials", "retraction_distance_mm", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(
        db, "materials", "retraction_speed_mm_s", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(db, "materials", "pressure_advance", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "print_speed_mm_s", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "xy_compensation_mm", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "humidity_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "drying_required", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "materials", "notes", "TEXT")
    ensure_column(db, "customers", "customer_type", "TEXT")
    ensure_column(db, "customers", "postal_code", "TEXT")
    ensure_column(db, "customers", "street", "TEXT")
    ensure_column(db, "customers", "address_number", "TEXT")
    ensure_column(db, "customers", "address_complement", "TEXT")
    ensure_column(db, "customers", "neighborhood", "TEXT")
    ensure_column(db, "customers", "state", "TEXT")
    ensure_column(db, "customers", "lead_source", "TEXT")
    ensure_column(db, "printers", "brand", "TEXT")
    ensure_column(db, "printers", "serial_number", "TEXT")
    ensure_column(db, "printers", "technology", "TEXT")
    ensure_column(db, "printers", "location", "TEXT")
    ensure_column(db, "printers", "purchase_date", "TEXT")
    ensure_column(db, "printers", "last_maintenance_date", "TEXT")
    ensure_column(db, "printers", "next_maintenance_date", "TEXT")
    ensure_column(db, "printers", "hourly_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "printers", "energy_watts", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "printers", "purchase_value", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "printers", "useful_life_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "printers", "has_ams", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "printers", "ams_model", "TEXT")
    ensure_column(db, "printers", "kwh_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(
        db, "printers", "monthly_maintenance_cost", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(
        db,
        "operational_cost_settings",
        "monthly_fixed_cost",
        "REAL NOT NULL DEFAULT 0",
    )
    ensure_column(
        db,
        "operational_cost_settings",
        "productive_hours_per_month",
        "REAL NOT NULL DEFAULT 0",
    )
    ensure_column(db, "operational_cost_settings", "notes", "TEXT")
    ensure_column(db, "products", "additional_material_types", "TEXT")
    ensure_column(db, "products", "accessories", "TEXT")
    ensure_column(db, "filament_dryers", "brand", "TEXT")
    ensure_column(db, "filament_dryers", "model", "TEXT")
    ensure_column(db, "filament_dryers", "dryer_type", "TEXT")
    ensure_column(db, "filament_dryers", "power_watts", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "filament_dryers", "useful_life_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "filament_dryers", "price", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "filament_dryers", "kwh_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "filament_dryers", "hourly_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "filament_dryers", "is_default", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(db, "components", "component_type", "TEXT")
    ensure_column(db, "components", "sku", "TEXT")
    ensure_column(db, "components", "manufacturer_name", "TEXT")
    normalize_existing_customer_data(db)
    db.commit()
    ensure_column(db, "components", "part_number", "TEXT")
    ensure_column(db, "components", "compatible_with", "TEXT")
    ensure_column(db, "components", "location", "TEXT")
    ensure_column(db, "components", "unit_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "product_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "shipping_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "store_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "coupon_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "payment_discount", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "real_total_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "unit_measure", "TEXT")
    ensure_column(db, "components", "stock_quantity", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "components", "minimum_quantity", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "commercial_entries", "document_number", "TEXT")
    ensure_column(db, "commercial_entries", "invoice_group_id", "TEXT")
    ensure_column(db, "commercial_entries", "entry_date", "TEXT")
    ensure_column(db, "commercial_entries", "order_number", "TEXT")
    ensure_column(db, "commercial_entries", "invoice_number", "TEXT")
    ensure_column(db, "commercial_entries", "product_name", "TEXT")
    ensure_column(db, "suppliers", "supplier_link", "TEXT")
    ensure_column(db, "components", "purchase_link", "TEXT")
    ensure_column(db, "components", "notes", "TEXT")
    migrate_accessories_to_components(db)
    ensure_column(db, "jobs", "customer_id", "INTEGER")
    ensure_column(db, "jobs", "representative_id", "INTEGER")
    ensure_column(db, "jobs", "partner_store_id", "INTEGER")
    ensure_column(db, "jobs", "due_date", "TEXT")
    ensure_column(db, "jobs", "quantity", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(db, "jobs", "sale_channel", "TEXT")
    ensure_column(db, "jobs", "customer_notes", "TEXT")
    ensure_column(db, "jobs", "internal_notes", "TEXT")
    ensure_column(db, "jobs", "labor_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "jobs", "labor_hourly_rate", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "jobs", "design_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "jobs", "design_hourly_rate", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "jobs", "valid_until", "TEXT")
    ensure_column(db, "jobs", "payment_terms", "TEXT")
    ensure_column(db, "jobs", "model_link", "TEXT")
    ensure_column(db, "jobs", "customer_document_token", "TEXT")
    ensure_column(db, "jobs", "production_document_token", "TEXT")
    ensure_column(db, "jobs", "printer_id", "INTEGER")
    ensure_column(db, "jobs", "filament_dryer_id", "INTEGER")
    ensure_column(db, "jobs", "dryer_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "jobs", "dryer_cost_per_hour", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "job_materials", "print_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "job_materials", "printer_id", "INTEGER")
    ensure_column(
        db, "job_materials", "energy_cost_per_hour", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(
        db, "job_materials", "operating_cost_per_hour", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(db, "job_materials", "filament_dryer_id", "INTEGER")
    ensure_column(db, "job_materials", "dryer_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(
        db, "job_materials", "dryer_cost_per_hour", "REAL NOT NULL DEFAULT 0"
    )
    ensure_column(db, "job_services", "addition_value", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "job_services", "discount_value", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "job_services", "category", "TEXT")
    ensure_column(db, "products", "sku", "TEXT")
    ensure_column(db, "products", "category", "TEXT")
    ensure_column(db, "products", "description", "TEXT")
    ensure_column(db, "products", "material_id", "INTEGER")
    ensure_column(db, "products", "weight_grams", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "print_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "energy_cost_per_hour", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "operating_cost_per_hour", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "labor_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "labor_hourly_rate", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "design_hours", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "design_hourly_rate", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "extra_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "margin_percent", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "unit_cost", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "sale_price", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "stock_quantity", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "minimum_quantity", "REAL NOT NULL DEFAULT 0")
    ensure_column(db, "products", "sale_channel", "TEXT")
    ensure_column(db, "products", "status", "TEXT NOT NULL DEFAULT 'Ativo'")
    ensure_column(db, "products", "model_link", "TEXT")
    ensure_column(db, "products", "photo_path", "TEXT")
    ensure_column(db, "products", "photo_original_name", "TEXT")
    ensure_column(db, "products", "notes", "TEXT")
    ensure_column(db, "products", "created_at", "TEXT")
    ensure_column(
        db,
        "inventory_movements",
        "unit_cost_per_kg",
        "REAL NOT NULL DEFAULT 0",
    )
    ensure_column(db, "inventory_movements", "related_job_id", "INTEGER")
    ensure_column(db, "jobs", "created_at", "TEXT")
    ensure_column(db, "payment_terms", "notes", "TEXT")
    ensure_column(db, "sales_channels", "notes", "TEXT")
    db.execute(
        "UPDATE jobs SET created_at = ? WHERE created_at IS NULL OR created_at = ''",
        (date.today().isoformat(),),
    )
    rows_without_customer_token = db.execute(
        "SELECT id FROM jobs WHERE customer_document_token IS NULL OR customer_document_token = ''"
    ).fetchall()
    for row in rows_without_customer_token:
        db.execute(
            "UPDATE jobs SET customer_document_token = ? WHERE id = ?",
            (make_public_document_token(), row["id"]),
        )
    rows_without_production_token = db.execute(
        "SELECT id FROM jobs WHERE production_document_token IS NULL OR production_document_token = ''"
    ).fetchall()
    for row in rows_without_production_token:
        db.execute(
            "UPDATE jobs SET production_document_token = ? WHERE id = ?",
            (make_public_document_token(), row["id"]),
        )
    db.execute(
        """
        INSERT OR IGNORE INTO operational_cost_settings (
            id,
            monthly_fixed_cost,
            productive_hours_per_month,
            notes
        )
        VALUES (1, 0, 0, '')
        """
    )
    seed_payment_terms(db)
    seed_sales_channels(db)
    seed_material_types(db)
    refresh_zero_component_unit_costs(db)

    db.commit()


def seed_payment_terms(db: sqlite3.Connection) -> None:
    default_terms = [
        "Pix a vista",
        "Cartao de credito",
        "Cartao de debito",
        "50% entrada e 50% na entrega",
        "Sinal de 30% e saldo na entrega",
        "Boleto 7 dias",
        "Boleto 15 dias",
        "Boleto 30 dias",
        "A combinar",
    ]
    for term in default_terms:
        db.execute(
            """
            INSERT INTO payment_terms (name)
            SELECT ?
            WHERE NOT EXISTS (
                SELECT 1 FROM payment_terms WHERE LOWER(name) = LOWER(?)
            )
            """,
            (term, term),
        )


def seed_sales_channels(db: sqlite3.Connection) -> None:
    default_channels = [
        "Instagram",
        "WhatsApp",
        "Site",
        "Loja fisica",
        "Mercado Livre",
        "Shopee",
        "Indicacao",
        "Representante",
        "Loja parceira",
    ]
    for channel in default_channels:
        db.execute(
            """
            INSERT INTO sales_channels (name)
            SELECT ?
            WHERE NOT EXISTS (
                SELECT 1 FROM sales_channels WHERE LOWER(name) = LOWER(?)
            )
            """,
            (channel, channel),
        )


def seed_material_types(db: sqlite3.Connection) -> None:
    default_types = [
        "PLA",
        "PETG",
        "ABS",
        "ASA",
        "TPU",
        "PA",
        "PC",
        "PVA",
        "HIPS",
        "Resina",
    ]
    for material_type in default_types:
        db.execute(
            """
            INSERT INTO material_types (name)
            SELECT ?
            WHERE NOT EXISTS (
                SELECT 1 FROM material_types WHERE LOWER(name) = LOWER(?)
            )
            """,
            (material_type, material_type),
        )


def migrate_accessories_to_components(db: sqlite3.Connection) -> None:
    has_accessories = db.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'accessories'
        """
    ).fetchone()
    if not has_accessories:
        return

    components_count = db.execute(
        "SELECT COUNT(*) AS total FROM components"
    ).fetchone()["total"]
    if components_count:
        return

    db.execute(
        """
        INSERT INTO components (
            name,
            component_type,
            sku,
            stock_quantity,
            minimum_quantity,
            notes
        )
        SELECT
            name,
            accessory_type,
            sku,
            stock_quantity,
            minimum_quantity,
            notes
        FROM accessories
        """
    )


def calculate_job_values(
    material_cost_per_kg: float,
    weight_grams: float,
    print_hours: float,
    energy_cost_per_hour: float,
    operating_cost_per_hour: float,
    extra_cost: float,
    margin_percent: float,
) -> tuple[float, float]:
    cost_per_gram = material_cost_per_kg / 1000
    material_cost = weight_grams * cost_per_gram
    energy_cost = print_hours * energy_cost_per_hour
    operating_cost = print_hours * operating_cost_per_hour
    total_cost = material_cost + energy_cost + operating_cost + extra_cost

    margin_ratio = margin_percent / 100
    if margin_ratio >= 1:
        suggested_price = total_cost
    else:
        suggested_price = total_cost / (1 - margin_ratio)

    return round(total_cost, 2), round(suggested_price, 2)


def calculate_price_with_margin(total_cost: float, margin_percent: float) -> float:
    margin_ratio = margin_percent / 100
    if margin_ratio >= 1:
        suggested_price = total_cost
    else:
        suggested_price = total_cost / (1 - margin_ratio)
    return round(suggested_price, 2)


def calculate_detailed_job_values(
    material_lines: list[dict[str, Any]],
    component_lines: list[dict[str, Any]],
    print_hours: float,
    energy_cost_per_hour: float,
    operating_cost_per_hour: float,
    dryer_hours: float,
    dryer_cost_per_hour: float,
    labor_hours: float,
    labor_hourly_rate: float,
    design_hours: float,
    design_hourly_rate: float,
    extra_cost: float,
    margin_percent: float,
) -> tuple[float, float]:
    material_cost = sum(
        float(line["weight_grams"]) * (float(line["cost_per_kg"]) / 1000)
        for line in material_lines
    )
    component_cost = sum(
        float(line["quantity"]) * float(line["unit_cost"])
        for line in component_lines
    )
    total_cost = (
        material_cost
        + component_cost
        + (print_hours * energy_cost_per_hour)
        + (print_hours * operating_cost_per_hour)
        + (dryer_hours * dryer_cost_per_hour)
        + (labor_hours * labor_hourly_rate)
        + (design_hours * design_hourly_rate)
        + extra_cost
    )
    return round(total_cost, 2), calculate_price_with_margin(total_cost, margin_percent)


def calculate_product_values(
    material_cost_per_kg: float,
    weight_grams: float,
    print_hours: float,
    energy_cost_per_hour: float,
    operating_cost_per_hour: float,
    labor_hours: float,
    labor_hourly_rate: float,
    design_hours: float,
    design_hourly_rate: float,
    extra_cost: float,
    margin_percent: float,
) -> tuple[float, float]:
    material_cost = weight_grams * (material_cost_per_kg / 1000)
    total_cost = (
        material_cost
        + (print_hours * energy_cost_per_hour)
        + (print_hours * operating_cost_per_hour)
        + (labor_hours * labor_hourly_rate)
        + (design_hours * design_hourly_rate)
        + extra_cost
    )
    return round(total_cost, 2), calculate_price_with_margin(total_cost, margin_percent)


def get_form_list(name: str) -> list[str]:
    values = request.form.getlist(name)
    if values:
        return values
    return request.form.getlist(f"{name}[]")


def build_job_material_lines(db: sqlite3.Connection) -> list[dict[str, Any]]:
    material_ids = get_form_list("material_id")
    weights = get_form_list("material_weight_grams") or get_form_list("weight_grams")
    print_hours_list = get_form_list("print_hours")
    printer_ids = get_form_list("printer_id")
    dryer_ids = get_form_list("filament_dryer_id")
    notes = get_form_list("material_notes")
    lines = []
    for index, material_id in enumerate(material_ids):
        if not material_id or material_id.startswith("__"):
            continue
        weight_grams = float(weights[index] or 0) if index < len(weights) else 0.0
        if weight_grams <= 0:
            continue
        print_hours = (
            float(print_hours_list[index] or 0) if index < len(print_hours_list) else 0.0
        )
        printer_id = int(printer_ids[index]) if index < len(printer_ids) and printer_ids[index] else None
        dryer_id = int(dryer_ids[index]) if index < len(dryer_ids) and dryer_ids[index] else None
        material = db.execute(
            "SELECT * FROM materials WHERE id = ?",
            (int(material_id),),
        ).fetchone()
        if material is None:
            continue
        printer = None
        energy_cost_per_hour = 0.0
        operating_cost_per_hour = 0.0
        if printer_id is not None:
            printer = db.execute("SELECT * FROM printers WHERE id = ?", (printer_id,)).fetchone()
            energy_cost_per_hour, operating_cost_per_hour = get_printer_cost_rates(printer)

        dryer = None
        dryer_cost_per_hour = 0.0
        dryer_hours = 0.0
        if dryer_id is not None:
            dryer = db.execute(
                "SELECT * FROM filament_dryers WHERE id = ?",
                (dryer_id,),
            ).fetchone()
            dryer_cost_per_hour = float(dryer["hourly_cost"] or 0) if dryer else 0.0
            dryer_hours = print_hours
        lines.append(
            {
                "material": material,
                "material_id": int(material_id),
                "material_name": material["name"],
                "weight_grams": weight_grams,
                "cost_per_kg": float(material["cost_per_kg"] or 0),
                "print_hours": print_hours,
                "printer_id": printer_id,
                "printer_label": (
                    f"{printer['name']} - {printer['model']}"
                    if printer and printer["model"]
                    else (printer["name"] if printer else "")
                ),
                "energy_cost_per_hour": energy_cost_per_hour,
                "operating_cost_per_hour": operating_cost_per_hour,
                "filament_dryer_id": dryer_id,
                "dryer_label": (
                    f"{dryer['brand']} {dryer['model']}".strip() if dryer else ""
                ),
                "dryer_hours": dryer_hours,
                "dryer_cost_per_hour": dryer_cost_per_hour,
                "notes": notes[index].strip() if index < len(notes) else "",
            }
        )
    return lines


def build_job_component_lines(db: sqlite3.Connection) -> list[dict[str, Any]]:
    component_ids = get_form_list("component_id")
    quantities = get_form_list("component_quantity")
    notes = get_form_list("component_notes")
    lines = []
    for index, component_id in enumerate(component_ids):
        if not component_id or component_id.startswith("__"):
            continue
        quantity = float(quantities[index] or 0) if index < len(quantities) else 0.0
        if quantity <= 0:
            continue
        component = db.execute(
            "SELECT * FROM components WHERE id = ?",
            (int(component_id),),
        ).fetchone()
        if component is None:
            continue
        lines.append(
            {
                "component": component,
                "component_id": int(component_id),
                "component_name": component["name"],
                "quantity": quantity,
                "unit_cost": float(component["unit_cost"] or 0),
                "unit_measure": component["unit_measure"] or "un",
                "notes": notes[index].strip() if index < len(notes) else "",
            }
        )
    return lines


def build_job_service_lines(db: sqlite3.Connection) -> list[dict[str, Any]]:
    names = get_form_list("service_name")
    item_names = get_form_list("item_name")
    product_ids = get_form_list("product_id")
    categories = get_form_list("service_category")
    quantities = get_form_list("service_quantity")
    hours = get_form_list("service_hours")
    unit_prices = get_form_list("service_unit_price")
    notes = get_form_list("service_notes")
    additions = get_form_list("service_additions")
    discounts = get_form_list("service_discounts")
    lines = []
    for index, service_name in enumerate(names):
        service_name = service_name.strip()
        item_name = item_names[index].strip() if index < len(item_names) else ""
        product_id = product_ids[index].strip() if index < len(product_ids) else ""
        category = categories[index].strip() if index < len(categories) else ""
        if not service_name:
            service_name = item_name
        if product_id and not category and not product_id.startswith("__"):
            product = db.execute(
                "SELECT category, name FROM products WHERE id = ?",
                (int(product_id),),
            ).fetchone()
            if product is not None:
                category = (product["category"] or "").strip()
                if not service_name:
                    service_name = (product["name"] or "").strip()
        if not service_name:
            continue
        quantity = float(quantities[index] or 1) if index < len(quantities) else 1.0
        service_hours = float(hours[index] or 0) if index < len(hours) else 0.0
        unit_price = (
            parse_brazilian_decimal(unit_prices[index])
            if index < len(unit_prices)
            else 0.0
        )
        addition = (
            parse_brazilian_decimal(additions[index])
            if index < len(additions)
            else 0.0
        )
        discount = (
            parse_brazilian_decimal(discounts[index])
            if index < len(discounts)
            else 0.0
        )
        total_price = round((quantity * unit_price) + addition - discount, 2)
        lines.append(
            {
                "service_name": service_name,
                "category": category,
                "quantity": quantity,
                "hours": service_hours,
                "unit_price": unit_price,
                "addition_value": addition,
                "discount_value": discount,
                "total_price": total_price,
                "notes": notes[index].strip() if index < len(notes) else "",
            }
        )
    return lines


def get_next_job_number(db: sqlite3.Connection) -> int:
    row = db.execute("SELECT IFNULL(MAX(id), 0) + 1 AS next_id FROM jobs").fetchone()
    return int(row["next_id"])


def save_job_photos(job_id: int) -> None:
    files = request.files.getlist("product_photos")
    if not files:
        return

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    db = get_db()
    for index, uploaded_file in enumerate(files, start=1):
        if not uploaded_file or not uploaded_file.filename:
            continue
        filename = secure_filename(uploaded_file.filename)
        if not filename:
            continue
        target_name = f"job-{job_id}-{index}-{filename}"
        target_path = UPLOAD_DIR / target_name
        uploaded_file.save(target_path)
        db.execute(
            """
            INSERT INTO job_photos (job_id, file_path, original_name)
            VALUES (?, ?, ?)
            """,
            (
                job_id,
                target_name,
                uploaded_file.filename,
            ),
        )


def save_product_photo(product_id: int) -> dict[str, str] | None:
    uploaded_file = request.files.get("product_photo")
    if not uploaded_file or not uploaded_file.filename:
        return None

    filename = secure_filename(uploaded_file.filename)
    if not filename:
        return None

    PRODUCT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target_name = f"product-{product_id}-{filename}"
    uploaded_file.save(PRODUCT_UPLOAD_DIR / target_name)
    return {
        "photo_path": target_name,
        "photo_original_name": uploaded_file.filename,
    }


def generate_sequential_code(
    db: sqlite3.Connection, table_name: str, column_name: str, prefix: str
) -> str:
    row = db.execute(
        f"""
        SELECT MAX(CAST(SUBSTR({column_name}, {len(prefix) + 2}) AS INTEGER)) AS last_number
        FROM {table_name}
        WHERE {column_name} LIKE ?
        """,
        (f"{prefix}-%",),
    ).fetchone()
    next_number = int(row["last_number"] or 0) + 1
    return f"{prefix}-{next_number:04d}"


def get_next_material_sequence_number(db: sqlite3.Connection) -> int:
    row = db.execute(
        """
        SELECT MAX(CAST(SUBSTR(sku, INSTR(sku, '-') + 1) AS INTEGER)) AS last_number
        FROM materials
        WHERE sku LIKE '%-%'
        """
    ).fetchone()
    return int(row["last_number"] or 0) + 1


def get_material_code_prefix(material_type: str | None) -> str:
    normalized = "".join(ch for ch in (material_type or "").strip().upper() if ch.isalnum())
    if len(normalized) >= 3:
        return normalized[:3]
    if normalized:
        return normalized.ljust(3, "X")
    return "MAT"


def build_material_code(material_type: str | None, sequence_number: int) -> str:
    return f"{get_material_code_prefix(material_type)}-{sequence_number:04d}"


def build_product_material_label(material: sqlite3.Row | dict[str, Any] | None) -> str:
    if not material:
        return ""
    color = str(material["color"] or "-").strip()
    material_type = str(material["material_type"] or "-").strip()
    line = str(material["line_series"] or material["name"] or "-").strip()
    manufacturer = str(material["manufacturer_name"] or "-").strip()
    return f"{color} / {material_type} / {line} / {manufacturer}"


@app.template_filter("material_label")
def material_label_filter(material: sqlite3.Row | dict[str, Any] | None) -> str:
    return build_product_material_label(material)


def build_product_component_label(component: sqlite3.Row | dict[str, Any] | None) -> str:
    if not component:
        return ""
    manufacturer_name = component["manufacturer_name"] if component["manufacturer_name"] else ""
    if manufacturer_name:
        return f"{component['name']} / {manufacturer_name}"
    return str(component["name"])


def parse_product_material_lines(
    raw_value: str | None,
    materials_by_id: dict[int, sqlite3.Row],
) -> list[dict[str, Any]]:
    if not raw_value:
        return []
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        payload = None

    if isinstance(payload, list):
        lines: list[dict[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            material_id = parse_integerish(str(entry.get("material_id") or ""))
            material = materials_by_id.get(material_id)
            label = (
                build_product_material_label(material)
                or str(entry.get("label") or "").strip()
            )
            lines.append(
                {
                    "material_id": material_id or None,
                    "label": label,
                    "quantity_grams": float(entry.get("quantity_grams") or 0),
                    "print_hours": float(entry.get("print_hours") or 0),
                }
            )
        return lines

    return [
        {
            "material_id": None,
            "label": line.strip(),
            "quantity_grams": 0.0,
            "print_hours": 0.0,
        }
        for line in str(raw_value).splitlines()
        if line.strip()
    ]


def parse_product_component_lines(
    raw_value: str | None,
    components_by_id: dict[int, sqlite3.Row],
) -> list[dict[str, Any]]:
    if not raw_value:
        return []
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        payload = None

    if isinstance(payload, list):
        lines: list[dict[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            component_id = parse_integerish(str(entry.get("component_id") or ""))
            component = components_by_id.get(component_id)
            label = (
                build_product_component_label(component)
                or str(entry.get("label") or "").strip()
            )
            lines.append(
                {
                    "component_id": component_id or None,
                    "label": label,
                    "quantity": float(entry.get("quantity") or 0),
                }
            )
        return lines

    return [
        {
            "component_id": None,
            "label": line.strip(),
            "quantity": 0.0,
        }
        for line in str(raw_value).splitlines()
        if line.strip()
    ]


def fetch_commercial_item_options(db: sqlite3.Connection) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    materials = db.execute(
        f"SELECT * FROM materials ORDER BY {material_order_clause()}"
    ).fetchall()
    components = db.execute(
        "SELECT * FROM components ORDER BY sku ASC, name ASC"
    ).fetchall()

    for material in materials:
        items.append(
            {
                "ref": f"material:{material['id']}",
                "kind": "material",
                "code": material["sku"] or "",
                "item_type": material["material_type"] or "",
                "brand_name": material["manufacturer_name"] or "",
                "line_description": material["line_series"] or material["name"] or "",
                "color_name": material["color"] or "",
                "unit_name": "g",
                "site": material["purchase_link"] or "",
                "label": build_product_material_label(material),
            }
        )

    for component in components:
        items.append(
            {
                "ref": f"component:{component['id']}",
                "kind": "component",
                "code": component["sku"] or "",
                "item_type": component["component_type"] or "",
                "brand_name": component["manufacturer_name"] or "",
                "line_description": component["name"] or component["part_number"] or "",
                "color_name": "-",
                "unit_name": component["unit_measure"] or "un",
                "site": component["purchase_link"] or "",
                "label": f"{component['sku'] or '-'} - {component['name']}",
            }
        )

    return sorted(items, key=lambda item: (item["code"], item["label"]))


def fetch_recent_commercial_entries(
    db: sqlite3.Connection, limit: int = 20
) -> list[sqlite3.Row]:
    return db.execute(
        """
        SELECT
            commercial_entries.*,
            COALESCE(commercial_entries.invoice_group_id, 'entry-' || commercial_entries.id) AS effective_group_id,
            suppliers.name AS supplier_name,
            suppliers.supplier_link AS supplier_link,
            CASE
                WHEN commercial_entries.item_kind = 'material' THEN 'g'
                WHEN commercial_entries.item_kind = 'component' THEN COALESCE(NULLIF(components.unit_measure, ''), 'un')
                ELSE '-'
            END AS unit_name
        FROM commercial_entries
        LEFT JOIN suppliers ON suppliers.id = commercial_entries.supplier_id
        LEFT JOIN components ON components.id = commercial_entries.component_id
        ORDER BY COALESCE(commercial_entries.invoice_date, commercial_entries.entry_date, commercial_entries.created_at) DESC,
                 commercial_entries.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_commercial_entry(
    db: sqlite3.Connection, entry_id: int
) -> sqlite3.Row | None:
    return db.execute(
        """
        SELECT
            commercial_entries.*,
            COALESCE(commercial_entries.invoice_group_id, 'entry-' || commercial_entries.id) AS effective_group_id,
            suppliers.name AS supplier_name,
            suppliers.supplier_link AS supplier_link,
            CASE
                WHEN commercial_entries.item_kind = 'material' THEN 'g'
                WHEN commercial_entries.item_kind = 'component' THEN COALESCE(NULLIF(components.unit_measure, ''), 'un')
                ELSE '-'
            END AS unit_name
        FROM commercial_entries
        LEFT JOIN suppliers ON suppliers.id = commercial_entries.supplier_id
        LEFT JOIN components ON components.id = commercial_entries.component_id
        WHERE commercial_entries.id = ?
        """,
        (entry_id,),
    ).fetchone()


def fetch_commercial_entry_group(
    db: sqlite3.Connection, entry: sqlite3.Row | None
) -> list[sqlite3.Row]:
    if entry is None:
        return []
    group_id = entry["invoice_group_id"] or f"entry-{entry['id']}"
    if entry["invoice_group_id"]:
        return db.execute(
            """
            SELECT
                commercial_entries.*,
                CASE
                    WHEN commercial_entries.item_kind = 'material' THEN 'g'
                    WHEN commercial_entries.item_kind = 'component' THEN COALESCE(NULLIF(components.unit_measure, ''), 'un')
                    ELSE '-'
                END AS unit_name
            FROM commercial_entries
            LEFT JOIN components ON components.id = commercial_entries.component_id
            WHERE commercial_entries.invoice_group_id = ?
            ORDER BY commercial_entries.id ASC
            """,
            (group_id,),
        ).fetchall()
    return [entry]


def commercial_group_summary(entries: list[sqlite3.Row] | list[dict[str, Any]]) -> dict[str, float]:
    return {
        "items": float(len(entries)),
        "quantity": round(sum(float(entry["quantity"] or 0) for entry in entries), 2),
        "amount": round(sum(float(entry["amount"] or 0) for entry in entries), 2),
        "freight": round(sum(float(entry["freight"] or 0) for entry in entries), 2),
        "tax": round(sum(float(entry["tax"] or 0) for entry in entries), 2),
        "discount": round(sum(float(entry["discount"] or 0) for entry in entries), 2),
        "total": round(sum(float(entry["total_amount"] or 0) for entry in entries), 2),
    }


def apply_commercial_entry_stock(
    db: sqlite3.Connection, entry: sqlite3.Row | dict[str, Any], direction: int
) -> None:
    quantity = float(entry["quantity"] or 0)
    unit_cost = float(entry["unit_cost"] or 0)
    amount = float(entry["amount"] or 0)
    total_amount = float(entry["total_amount"] or 0)
    site = str(entry["site"] or "").strip()
    supplier_id = parse_integerish(entry["supplier_id"])
    notes = str(entry["notes"] or "").strip()

    if entry["item_kind"] == "material" and entry["material_id"]:
        db.execute(
            """
            UPDATE materials
            SET
                stock_grams = stock_grams + ?,
                cost_per_kg = CASE WHEN ? > 0 THEN ? ELSE cost_per_kg END,
                supplier_id = CASE WHEN ? IS NOT NULL THEN ? ELSE supplier_id END,
                purchase_link = CASE WHEN ? <> '' AND ? > 0 THEN ? ELSE purchase_link END
            WHERE id = ?
            """,
            (
                direction * quantity,
                direction,
                unit_cost * 1000,
                supplier_id,
                supplier_id,
                site,
                direction,
                site,
                entry["material_id"],
            ),
        )
        if direction > 0:
            db.execute(
                """
                INSERT INTO inventory_movements (
                    material_id,
                    movement_type,
                    quantity_grams,
                    unit_cost_per_kg,
                    notes
                )
                VALUES (?, 'Entrada', ?, ?, ?)
                """,
                (
                    entry["material_id"],
                    quantity,
                    unit_cost * 1000,
                    f"Nota fiscal de compra. {notes}".strip(),
                ),
            )
        else:
            db.execute(
                """
                DELETE FROM inventory_movements
                WHERE id IN (
                    SELECT id
                    FROM inventory_movements
                    WHERE material_id = ?
                      AND movement_type = 'Entrada'
                      AND ABS(quantity_grams - ?) < 0.000001
                      AND ABS(unit_cost_per_kg - ?) < 0.000001
                    ORDER BY id DESC
                    LIMIT 1
                )
                """,
                (entry["material_id"], quantity, unit_cost * 1000),
            )
    elif entry["item_kind"] == "component" and entry["component_id"]:
        db.execute(
            """
            UPDATE components
            SET
                stock_quantity = stock_quantity + ?,
                unit_cost = CASE WHEN ? > 0 THEN ? ELSE unit_cost END,
                product_cost = CASE WHEN ? > 0 THEN ? ELSE product_cost END,
                real_total_cost = CASE WHEN ? > 0 THEN ? ELSE real_total_cost END,
                purchase_link = CASE WHEN ? <> '' AND ? > 0 THEN ? ELSE purchase_link END
            WHERE id = ?
            """,
            (
                direction * quantity,
                direction,
                unit_cost,
                direction,
                amount,
                direction,
                total_amount,
                site,
                direction,
                site,
                entry["component_id"],
            ),
        )


def build_commercial_entries_from_form(
    item_options: list[dict[str, Any]], group_id: str | None = None
) -> list[dict[str, Any]]:
    item_refs = get_form_list("item_ref")
    quantities = get_form_list("quantity")
    amounts = get_form_list("amount")
    freights = get_form_list("freight")
    taxes = get_form_list("tax")
    discounts = get_form_list("discount")
    invoice_freight_total = parse_brazilian_decimal(
        request.form.get("invoice_freight_total")
    )
    invoice_tax_total = parse_brazilian_decimal(request.form.get("invoice_tax_total"))
    invoice_discount_total = parse_brazilian_decimal(
        request.form.get("invoice_discount_total")
    )

    entry_date = request.form.get("entry_date") or None
    invoice_date = request.form.get("invoice_date") or None
    order_number = request.form.get("order_number", "").strip()
    invoice_number = request.form.get("invoice_number", "").strip()
    document_number = invoice_number or request.form.get("document_number", "").strip()
    supplier_id = parse_integerish(request.form.get("supplier_id")) or None
    product_name = request.form.get("product_name", "").strip()
    site = request.form.get("site", "").strip()
    notes = request.form.get("notes", "").strip()
    invoice_group_id = group_id or uuid.uuid4().hex

    entries: list[dict[str, Any]] = []
    for index, item_ref in enumerate(item_refs):
        item_ref = (item_ref or "").strip()
        quantity = (
            parse_loose_float(quantities[index], 0.0)
            if index < len(quantities)
            else 0.0
        )
        if ":" not in item_ref or quantity <= 0:
            continue

        item_kind, raw_item_id = item_ref.split(":", 1)
        item_id = parse_integerish(raw_item_id)
        selected_option = next(
            (option for option in item_options if option["ref"] == item_ref), None
        )
        amount = (
            parse_brazilian_decimal(amounts[index]) if index < len(amounts) else 0.0
        )
        raw_freight = (
            parse_brazilian_decimal(freights[index]) if index < len(freights) else 0.0
        )
        raw_tax = parse_brazilian_decimal(taxes[index]) if index < len(taxes) else 0.0
        raw_discount = (
            parse_brazilian_decimal(discounts[index]) if index < len(discounts) else 0.0
        )
        total_amount = max(amount + raw_freight + raw_tax - raw_discount, 0)
        unit_cost = total_amount / quantity if quantity > 0 else 0

        entries.append(
            {
                "invoice_group_id": invoice_group_id,
                "entry_date": entry_date,
                "invoice_date": invoice_date,
                "order_number": order_number,
                "invoice_number": invoice_number,
                "document_number": document_number,
                "supplier_id": supplier_id,
                "item_kind": item_kind,
                "item_type": selected_option["item_type"] if selected_option else "",
                "material_id": item_id if item_kind == "material" else None,
                "component_id": item_id if item_kind == "component" else None,
                "item_code": selected_option["code"] if selected_option else "",
                "brand_name": selected_option["brand_name"] if selected_option else "",
                "line_description": (
                    selected_option["line_description"] if selected_option else ""
                ),
                "color_name": selected_option["color_name"] if selected_option else "",
                "quantity": quantity,
                "amount": amount,
                "freight": raw_freight,
                "tax": raw_tax,
                "discount": raw_discount,
                "total_amount": total_amount,
                "unit_cost": unit_cost,
                "site": site,
                "product_name": product_name,
                "notes": notes,
            }
        )
    if entries:
        amount_total = sum(float(entry["amount"] or 0) for entry in entries)
        if amount_total > 0:
            line_freight_total = sum(float(entry["freight"] or 0) for entry in entries)
            line_tax_total = sum(float(entry["tax"] or 0) for entry in entries)
            line_discount_total = sum(float(entry["discount"] or 0) for entry in entries)
            if invoice_freight_total or line_freight_total:
                line_freight_total = invoice_freight_total
            if invoice_tax_total or line_tax_total:
                line_tax_total = invoice_tax_total
            if invoice_discount_total or line_discount_total:
                line_discount_total = invoice_discount_total

            for entry in entries:
                share = float(entry["amount"] or 0) / amount_total
                entry["freight"] = round(line_freight_total * share, 2)
                entry["tax"] = round(line_tax_total * share, 2)
                entry["discount"] = round(line_discount_total * share, 2)
                entry["total_amount"] = max(
                    entry["amount"] + entry["freight"] + entry["tax"] - entry["discount"],
                    0,
                )
                entry["unit_cost"] = (
                    entry["total_amount"] / entry["quantity"]
                    if entry["quantity"] > 0
                    else 0
                )
    return entries


def insert_commercial_entries(
    db: sqlite3.Connection, entries: list[dict[str, Any]]
) -> None:
    for entry_data in entries:
        cursor = db.execute(
            """
            INSERT INTO commercial_entries (
                invoice_group_id,
                entry_date,
                invoice_date,
                order_number,
                invoice_number,
                document_number,
                supplier_id,
                item_kind,
                item_type,
                material_id,
                component_id,
                item_code,
                brand_name,
                line_description,
                color_name,
                quantity,
                amount,
                freight,
                tax,
                discount,
                total_amount,
                unit_cost,
                site,
                product_name,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(entry_data.values()),
        )
        entry_data["id"] = cursor.lastrowid
        apply_commercial_entry_stock(db, entry_data, 1)


def calculate_material_costs(
    stock_grams: float,
    product_cost: float,
    shipping_cost: float,
    store_discount: float,
    coupon_discount: float,
    payment_discount: float,
) -> tuple[float, float]:
    real_total_cost = max(
        product_cost + shipping_cost - store_discount - coupon_discount - payment_discount,
        0,
    )
    if stock_grams <= 0:
        cost_per_kg = 0.0
    else:
        cost_per_kg = real_total_cost * (1000 / stock_grams)
    return round(real_total_cost, 2), round(cost_per_kg, 2)


def calculate_component_costs(
    stock_quantity: float,
    product_cost: float,
    shipping_cost: float,
    store_discount: float,
    coupon_discount: float,
    payment_discount: float,
) -> tuple[float, float]:
    real_total_cost = max(
        product_cost + shipping_cost - store_discount - coupon_discount - payment_discount,
        0,
    )
    if stock_quantity <= 0:
        unit_cost = 0.0
    else:
        unit_cost = real_total_cost / stock_quantity
    return round(real_total_cost, 2), round(unit_cost, 4)


def refresh_zero_component_unit_costs(db: sqlite3.Connection) -> None:
    db.execute(
        """
        UPDATE components
        SET unit_cost = ROUND(real_total_cost / stock_quantity, 4)
        WHERE unit_cost = 0
          AND real_total_cost > 0
          AND stock_quantity > 0
        """
    )


def calculate_printer_hourly_cost(
    purchase_value: float,
    useful_life_hours: float,
    energy_watts: float,
    kwh_cost: float,
    monthly_maintenance_cost: float = 0.0,
    monthly_fixed_cost: float = 0.0,
    productive_hours_per_month: float = 0.0,
) -> float:
    breakdown = calculate_printer_cost_breakdown(
        purchase_value=purchase_value,
        useful_life_hours=useful_life_hours,
        energy_watts=energy_watts,
        kwh_cost=kwh_cost,
        monthly_maintenance_cost=monthly_maintenance_cost,
        monthly_fixed_cost=monthly_fixed_cost,
        productive_hours_per_month=productive_hours_per_month,
    )
    return round(breakdown["total_hourly_cost"], 2)


def calculate_shared_operating_hourly_cost(
    monthly_fixed_cost: float, productive_hours_per_month: float
) -> float:
    if productive_hours_per_month <= 0:
        return 0.0
    return round(monthly_fixed_cost / productive_hours_per_month, 4)


def calculate_printer_cost_breakdown(
    purchase_value: float,
    useful_life_hours: float,
    energy_watts: float,
    kwh_cost: float,
    monthly_maintenance_cost: float = 0.0,
    monthly_fixed_cost: float = 0.0,
    productive_hours_per_month: float = 0.0,
) -> dict[str, float]:
    depreciation_cost = 0.0
    if useful_life_hours > 0:
        depreciation_cost = purchase_value / useful_life_hours
    energy_cost = (energy_watts / 1000) * kwh_cost
    maintenance_hourly_cost = (
        monthly_maintenance_cost / productive_hours_per_month
        if productive_hours_per_month > 0
        else 0.0
    )
    shared_overhead_hourly_cost = calculate_shared_operating_hourly_cost(
        monthly_fixed_cost, productive_hours_per_month
    )
    operating_hourly_cost = (
        depreciation_cost + maintenance_hourly_cost + shared_overhead_hourly_cost
    )
    total_hourly_cost = operating_hourly_cost + energy_cost
    return {
        "depreciation_hourly_cost": round(depreciation_cost, 4),
        "energy_hourly_cost": round(energy_cost, 4),
        "maintenance_hourly_cost": round(maintenance_hourly_cost, 4),
        "shared_overhead_hourly_cost": round(shared_overhead_hourly_cost, 4),
        "operating_hourly_cost": round(operating_hourly_cost, 4),
        "total_hourly_cost": round(total_hourly_cost, 4),
    }


def get_operational_cost_settings(db: sqlite3.Connection) -> sqlite3.Row:
    row = db.execute(
        "SELECT * FROM operational_cost_settings WHERE id = 1"
    ).fetchone()
    if row is None:
        db.execute(
            """
            INSERT INTO operational_cost_settings (
                id,
                monthly_fixed_cost,
                productive_hours_per_month,
                notes
            )
            VALUES (1, 0, 0, '')
            """
        )
        db.commit()
        row = db.execute(
            "SELECT * FROM operational_cost_settings WHERE id = 1"
        ).fetchone()
    return row


def get_default_product_cost_rates(db: sqlite3.Connection) -> tuple[float, float]:
    settings = get_operational_cost_settings(db)
    printers = db.execute("SELECT * FROM printers ORDER BY name ASC").fetchall()
    rates = [
        get_printer_cost_rates(printer, settings)
        for printer in printers
        if printer is not None
    ]
    populated_rates = [
        (energy_rate, operating_rate)
        for energy_rate, operating_rate in rates
        if energy_rate > 0 or operating_rate > 0
    ]
    if populated_rates:
        energy_average = sum(rate[0] for rate in populated_rates) / len(populated_rates)
        operating_average = sum(rate[1] for rate in populated_rates) / len(
            populated_rates
        )
        return round(energy_average, 4), round(operating_average, 4)
    return 0.0, calculate_shared_operating_hourly_cost(
        float(settings["monthly_fixed_cost"] or 0),
        float(settings["productive_hours_per_month"] or 0),
    )


def normalize_phone_for_whatsapp(phone: str | None) -> str:
    digits = "".join(character for character in str(phone or "") if character.isdigit())
    if not digits:
        return ""
    if digits.startswith("55"):
        return digits
    if len(digits) in {10, 11}:
        return f"55{digits}"
    return digits


def build_whatsapp_link(phone: str | None, message: str) -> str:
    phone_digits = normalize_phone_for_whatsapp(phone)
    encoded_message = quote(message)
    if phone_digits:
        return f"https://wa.me/{phone_digits}?text={encoded_message}"
    return f"https://wa.me/?text={encoded_message}"


def make_public_document_token() -> str:
    return uuid.uuid4().hex


def get_printer_cost_rates(
    printer: sqlite3.Row | None, operational_settings: sqlite3.Row | None = None
) -> tuple[float, float]:
    if printer is None:
        return 0.0, 0.0
    settings = operational_settings
    if settings is None:
        settings = get_operational_cost_settings(get_db())
    breakdown = calculate_printer_cost_breakdown(
        purchase_value=float(printer["purchase_value"] or 0),
        useful_life_hours=float(printer["useful_life_hours"] or 0),
        energy_watts=float(printer["energy_watts"] or 0),
        kwh_cost=float(printer["kwh_cost"] or 0),
        monthly_maintenance_cost=float(printer["monthly_maintenance_cost"] or 0),
        monthly_fixed_cost=float(settings["monthly_fixed_cost"] or 0),
        productive_hours_per_month=float(settings["productive_hours_per_month"] or 0),
    )
    return (
        breakdown["energy_hourly_cost"],
        breakdown["operating_hourly_cost"],
    )


def summarize_cost_lines(
    material_lines: list[dict[str, Any]],
    component_lines: list[dict[str, Any]],
    labor_hours: float,
    labor_hourly_rate: float,
    design_hours: float,
    design_hourly_rate: float,
    extra_cost: float,
    sale_total: float,
) -> dict[str, Any]:
    material_breakdown = []
    energy_breakdown = []
    operating_breakdown = []
    dryer_breakdown = []
    component_breakdown = []

    material_cost = 0.0
    energy_cost = 0.0
    operating_cost = 0.0
    dryer_cost = 0.0
    total_weight = 0.0
    total_print_hours = 0.0
    total_dryer_hours = 0.0

    for line in material_lines:
        weight_grams = float(line.get("weight_grams") or 0)
        cost_per_kg = float(line.get("cost_per_kg") or 0)
        line_material_cost = round((weight_grams * cost_per_kg) / 1000, 2)
        line_print_hours = float(line.get("print_hours") or 0)
        line_energy_rate = float(line.get("energy_cost_per_hour") or 0)
        line_operating_rate = float(line.get("operating_cost_per_hour") or 0)
        line_dryer_hours = float(line.get("dryer_hours") or 0)
        line_dryer_rate = float(line.get("dryer_cost_per_hour") or 0)
        line_energy_total = round(line_print_hours * line_energy_rate, 2)
        line_operating_total = round(line_print_hours * line_operating_rate, 2)
        line_dryer_total = round(line_dryer_hours * line_dryer_rate, 2)

        material_cost += line_material_cost
        energy_cost += line_energy_total
        operating_cost += line_operating_total
        dryer_cost += line_dryer_total
        total_weight += weight_grams
        total_print_hours += line_print_hours
        total_dryer_hours += line_dryer_hours

        material_breakdown.append(
            {
                "label": " / ".join(
                    [
                        part
                        for part in [
                            line.get("material_type"),
                            line.get("color"),
                            line.get("manufacturer_name"),
                        ]
                        if part
                    ]
                )
                or line.get("material_name")
                or line.get("name")
                or line.get("notes")
                or "Material",
                "base": f"{br_decimal(weight_grams)} g",
                "rate": f"R$ {br_money(cost_per_kg)}/kg",
                "total": line_material_cost,
            }
        )

        printer_label = line.get("printer_label") or "Sem impressora"
        energy_breakdown.append(
            {
                "label": printer_label,
                "base": f"{br_decimal(line_print_hours)} h",
                "rate": f"R$ {br_money(line_energy_rate)}/h",
                "total": line_energy_total,
            }
        )
        operating_breakdown.append(
            {
                "label": printer_label,
                "base": f"{br_decimal(line_print_hours)} h",
                "rate": f"R$ {br_money(line_operating_rate)}/h",
                "total": line_operating_total,
            }
        )
        if line_dryer_hours > 0 or line.get("dryer_label"):
            dryer_breakdown.append(
                {
                    "label": line.get("dryer_label") or "Sem secador",
                    "base": f"{br_decimal(line_dryer_hours)} h",
                    "rate": f"R$ {br_money(line_dryer_rate)}/h",
                    "total": line_dryer_total,
                }
            )

    component_cost = 0.0
    component_count = 0.0
    for line in component_lines:
        quantity = float(line.get("quantity") or 0)
        unit_cost = float(line.get("unit_cost") or 0)
        line_total = round(quantity * unit_cost, 2)
        component_cost += line_total
        component_count += quantity
        component_breakdown.append(
            {
                "label": line.get("component_name") or "Componente",
                "base": f"{br_decimal(quantity)} {line.get('unit_measure') or 'un'}",
                "rate": f"R$ {br_decimal(unit_cost, 4)}/un",
                "total": line_total,
            }
        )

    labor_cost = round(labor_hours * labor_hourly_rate, 2)
    design_cost = round(design_hours * design_hourly_rate, 2)
    total_cost = round(
        material_cost
        + component_cost
        + energy_cost
        + operating_cost
        + dryer_cost
        + labor_cost
        + design_cost
        + extra_cost,
        2,
    )

    return {
        "material_cost": round(material_cost, 2),
        "component_cost": round(component_cost, 2),
        "energy_cost": round(energy_cost, 2),
        "operating_cost": round(operating_cost, 2),
        "dryer_cost": round(dryer_cost, 2),
        "labor_cost": labor_cost,
        "design_cost": design_cost,
        "extra_cost": round(extra_cost, 2),
        "total_cost": total_cost,
        "suggested_price": round(sale_total, 2),
        "profit": round(sale_total - total_cost, 2),
        "total_weight": round(total_weight, 2),
        "total_print_hours": round(total_print_hours, 2),
        "total_dryer_hours": round(total_dryer_hours, 2),
        "component_count": round(component_count, 2),
        "breakdowns": {
            "materials": material_breakdown,
            "components": component_breakdown,
            "energy": energy_breakdown,
            "operating": operating_breakdown,
            "dryers": dryer_breakdown,
        },
    }


def parse_brazilian_decimal(value: str | None) -> float:
    if not value:
        return 0.0
    normalized = value.strip().replace("R$", "").replace(" ", "")
    if normalized.startswith("__"):
        return 0.0
    if "," in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    return float(normalized or 0)


def parse_integerish(value: str | None, default: int = 0) -> int:
    if value in (None, ""):
        return default
    normalized = str(value).strip().replace(",", ".")
    if normalized.startswith("__"):
        return default
    return int(float(normalized))


def inventory_delta_for_type(movement_type: str, quantity_grams: float) -> float:
    negative_movements = {"Ajuste negativo", "Consumo manual", "Perda"}
    return -quantity_grams if movement_type in negative_movements else quantity_grams


def inventory_direction_label(movement_type: str) -> str:
    negative_movements = {"Ajuste negativo", "Consumo manual", "Perda"}
    return "Saida" if movement_type in negative_movements else "Entrada"


def append_query_value(url: str, key: str, value: Any) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = str(value)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query),
            parts.fragment,
        )
    )


@app.template_filter("br_money")
def br_money(value: float | int | None) -> str:
    value = float(value or 0)
    formatted = f"{value:,.2f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


@app.template_filter("br_decimal")
def br_decimal(value: float | int | None, places: int = 2) -> str:
    value = float(value or 0)
    formatted = f"{value:,.{places}f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


@app.template_filter("br_date")
def br_date(value: str | None) -> str:
    if not value:
        return "-"
    raw = str(value).strip()
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return f"{raw[8:10]}/{raw[5:7]}/{raw[0:4]}"
    return raw


def fetch_reference_data(db: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    return {
        "customers": db.execute("SELECT * FROM customers ORDER BY name ASC").fetchall(),
        "suppliers": db.execute("SELECT * FROM suppliers ORDER BY name ASC").fetchall(),
        "representatives": db.execute(
            "SELECT * FROM representatives ORDER BY name ASC"
        ).fetchall(),
        "partner_stores": db.execute(
            "SELECT * FROM partner_stores ORDER BY name ASC"
        ).fetchall(),
        "payment_terms": db.execute(
            "SELECT * FROM payment_terms ORDER BY name ASC"
        ).fetchall(),
        "sales_channels": db.execute(
            "SELECT * FROM sales_channels ORDER BY name ASC"
        ).fetchall(),
        "material_types": db.execute(
            "SELECT * FROM material_types ORDER BY name ASC"
        ).fetchall(),
        "printers": db.execute("SELECT * FROM printers ORDER BY name ASC").fetchall(),
        "filament_dryers": db.execute(
            "SELECT * FROM filament_dryers ORDER BY brand ASC, model ASC"
        ).fetchall(),
        "components": db.execute(
            "SELECT * FROM components ORDER BY name ASC"
        ).fetchall(),
        "products": db.execute("SELECT * FROM products ORDER BY name ASC").fetchall(),
    }


def build_registry_menu(
    db: sqlite3.Connection, references: dict[str, list[sqlite3.Row]]
) -> list[dict[str, Any]]:
    materials_count = db.execute("SELECT COUNT(*) AS total FROM materials").fetchone()[
        "total"
    ]
    operational_settings = get_operational_cost_settings(db)
    shared_operating_rate = calculate_shared_operating_hourly_cost(
        float(operational_settings["monthly_fixed_cost"] or 0),
        float(operational_settings["productive_hours_per_month"] or 0),
    )
    return [
        {
            "title": "Estrutura produtiva",
            "items": [
                {
                    "icon": "◈",
                    "label": "Filamentos e materiais",
                    "count": materials_count,
                    "href": url_for("materials"),
                },
                {
                    "icon": "▤",
                    "label": "Impressoras",
                    "count": len(references["printers"]),
                    "href": url_for("registry_page", section="printers"),
                },
                {
                    "icon": "◐",
                    "label": "Custos operacionais",
                    "count": f"R$ {br_money(shared_operating_rate)}/h",
                    "href": url_for("registry_page", section="operational-costs"),
                },
                {
                    "icon": "▥",
                    "label": "Secador de filamentos",
                    "count": len(references["filament_dryers"]),
                    "href": url_for("registry_page", section="filament-dryers"),
                },
                {
                    "icon": "✦",
                    "label": "Componentes",
                    "count": len(references["components"]),
                    "href": url_for("registry_page", section="components"),
                },
                {
                    "icon": "▧",
                    "label": "Produtos para venda",
                    "count": len(references["products"]),
                    "href": url_for("products"),
                },
                {
                    "icon": "◍",
                    "label": "Tipos de filamento",
                    "count": len(references["material_types"]),
                    "href": url_for("registry_page", section="material-types"),
                },
            ],
        },
        {
            "title": "Rede comercial",
            "items": [
                {
                    "icon": "◎",
                    "label": "Clientes",
                    "count": len(references["customers"]),
                    "href": url_for("registry_page", section="customers"),
                },
                {
                    "icon": "◇",
                    "label": "Fornecedores",
                    "count": len(references["suppliers"]),
                    "href": url_for("registry_page", section="suppliers"),
                },
                {
                    "icon": "↗",
                    "label": "Representantes",
                    "count": len(references["representatives"]),
                    "href": url_for("registry_page", section="representatives"),
                },
                {
                    "icon": "▣",
                    "label": "Lojas parceiras",
                    "count": len(references["partner_stores"]),
                    "href": url_for("registry_page", section="partner-stores"),
                },
                {
                    "icon": "◉",
                    "label": "Condicoes de pagamento",
                    "count": len(references["payment_terms"]),
                    "href": url_for("registry_page", section="payment-terms"),
                },
                {
                    "icon": "◌",
                    "label": "Canais de venda",
                    "count": len(references["sales_channels"]),
                    "href": url_for("registry_page", section="sales-channels"),
                },
            ],
        },
    ]


def build_operational_cost_settings_form_data() -> dict[str, Any]:
    monthly_fixed_cost = parse_brazilian_decimal(request.form.get("monthly_fixed_cost"))
    productive_hours_per_month = parse_loose_float(
        request.form.get("productive_hours_per_month"), 0.0
    )
    notes = request.form.get("notes", "").strip()
    return {
        "monthly_fixed_cost": monthly_fixed_cost,
        "productive_hours_per_month": productive_hours_per_month,
        "notes": notes,
    }


def handle_registry_submission(db: sqlite3.Connection, section: str) -> int | None:
    if section == "customers":
        cursor = db.execute(
            """
            INSERT INTO customers (
                name,
                document,
                phone,
                email,
                customer_type,
                postal_code,
                street,
                address_number,
                address_complement,
                neighborhood,
                city,
                state,
                lead_source,
                segment,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalize_upper_text(request.form["name"]),
                normalize_upper_text(request.form["document"]),
                request.form["phone"].strip(),
                request.form["email"].strip().lower(),
                normalize_upper_text(request.form.get("customer_type", "")),
                request.form.get("postal_code", "").strip(),
                normalize_upper_text(request.form.get("street", "")),
                normalize_upper_text(request.form.get("address_number", "")),
                normalize_upper_text(request.form.get("address_complement", "")),
                normalize_upper_text(request.form.get("neighborhood", "")),
                normalize_upper_text(request.form["city"]),
                normalize_upper_text(request.form.get("state", "")),
                normalize_upper_text(request.form.get("lead_source", "")),
                normalize_upper_text(request.form.get("segment", "")),
                normalize_upper_text(request.form["notes"]),
            ),
        )
    elif section == "suppliers":
        cursor = db.execute(
            """
            INSERT INTO suppliers (name, contact_name, phone, email, supplier_link, lead_time_days, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["contact_name"].strip(),
                request.form["phone"].strip(),
                request.form["email"].strip(),
                request.form.get("supplier_link", "").strip(),
                int(request.form["lead_time_days"] or 0),
                request.form["notes"].strip(),
            ),
        )
    elif section == "representatives":
        cursor = db.execute(
            """
            INSERT INTO representatives (name, phone, email, commission_percent, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["phone"].strip(),
                request.form["email"].strip(),
                float(request.form["commission_percent"] or 0),
                request.form["notes"].strip(),
            ),
        )
    elif section == "partner-stores":
        cursor = db.execute(
            """
            INSERT INTO partner_stores (name, city, contact_name, phone, instagram, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["city"].strip(),
                request.form["contact_name"].strip(),
                request.form["phone"].strip(),
                request.form["instagram"].strip(),
                request.form["notes"].strip(),
            ),
        )
    elif section == "payment-terms":
        cursor = db.execute(
            """
            INSERT INTO payment_terms (name, notes)
            VALUES (?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
            ),
        )
    elif section == "sales-channels":
        cursor = db.execute(
            """
            INSERT INTO sales_channels (name, notes)
            VALUES (?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
            ),
        )
    elif section == "material-types":
        cursor = db.execute(
            """
            INSERT INTO material_types (name, notes)
            VALUES (?, ?)
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
            ),
        )
    elif section == "operational-costs":
        settings_data = build_operational_cost_settings_form_data()
        db.execute(
            """
            INSERT INTO operational_cost_settings (
                id,
                monthly_fixed_cost,
                productive_hours_per_month,
                notes
            )
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                monthly_fixed_cost = excluded.monthly_fixed_cost,
                productive_hours_per_month = excluded.productive_hours_per_month,
                notes = excluded.notes
            """,
            (
                settings_data["monthly_fixed_cost"],
                settings_data["productive_hours_per_month"],
                settings_data["notes"],
            ),
        )
        db.commit()
        return 1
    elif section == "printers":
        printer_data = build_printer_form_data(db)
        cursor = db.execute(
            """
            INSERT INTO printers (
                name,
                brand,
                model,
                serial_number,
                technology,
                nozzle_size,
                build_volume,
                location,
                status,
                purchase_date,
                last_maintenance_date,
                next_maintenance_date,
                hourly_cost,
                energy_watts,
                purchase_value,
                useful_life_hours,
                monthly_maintenance_cost,
                has_ams,
                ams_model,
                kwh_cost,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                printer_data["name"],
                printer_data["brand"],
                printer_data["model"],
                printer_data["serial_number"],
                printer_data["technology"],
                printer_data["nozzle_size"],
                printer_data["build_volume"],
                printer_data["location"],
                printer_data["status"],
                printer_data["purchase_date"],
                printer_data["last_maintenance_date"],
                printer_data["next_maintenance_date"],
                printer_data["hourly_cost"],
                printer_data["energy_watts"],
                printer_data["purchase_value"],
                printer_data["useful_life_hours"],
                printer_data["monthly_maintenance_cost"],
                printer_data["has_ams"],
                printer_data["ams_model"],
                printer_data["kwh_cost"],
                printer_data["notes"],
            ),
        )
    elif section == "filament-dryers":
        dryer_data = build_filament_dryer_form_data()
        cursor = db.execute(
            """
            INSERT INTO filament_dryers (
                brand,
                model,
                dryer_type,
                power_watts,
                useful_life_hours,
                price,
                kwh_cost,
                hourly_cost,
                is_default
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                dryer_data["brand"],
                dryer_data["model"],
                dryer_data["dryer_type"],
                dryer_data["power_watts"],
                dryer_data["useful_life_hours"],
                dryer_data["price"],
                dryer_data["kwh_cost"],
                dryer_data["hourly_cost"],
            ),
        )
    elif section == "components":
        component_code = generate_sequential_code(db, "components", "sku", "C")
        component_data = build_component_form_data()
        cursor = db.execute(
            """
            INSERT INTO components (
                name,
                component_type,
                sku,
                manufacturer_name,
                part_number,
                location,
                unit_cost,
                product_cost,
                shipping_cost,
                store_discount,
                coupon_discount,
                payment_discount,
                real_total_cost,
                unit_measure,
                stock_quantity,
                minimum_quantity,
                purchase_link,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                component_data["name"],
                component_data["component_type"],
                component_code,
                component_data["manufacturer_name"],
                component_data["part_number"],
                component_data["location"],
                component_data["unit_cost"],
                component_data["product_cost"],
                component_data["shipping_cost"],
                component_data["store_discount"],
                component_data["coupon_discount"],
                component_data["payment_discount"],
                component_data["real_total_cost"],
                component_data["unit_measure"],
                component_data["stock_quantity"],
                component_data["minimum_quantity"],
                component_data["purchase_link"],
                component_data["notes"],
            ),
        )
    else:
        raise KeyError(section)

    db.commit()
    return int(cursor.lastrowid) if cursor.lastrowid is not None else None


def build_printer_form_data(db: sqlite3.Connection) -> dict[str, Any]:
    settings = get_operational_cost_settings(db)
    purchase_value = parse_brazilian_decimal(request.form.get("purchase_value"))
    useful_life_hours = float(request.form["useful_life_hours"] or 0)
    energy_watts = float(request.form["energy_watts"] or 0)
    kwh_cost = parse_brazilian_decimal(request.form.get("kwh_cost"))
    monthly_maintenance_cost = parse_brazilian_decimal(
        request.form.get("monthly_maintenance_cost")
    )
    hourly_cost = calculate_printer_hourly_cost(
        purchase_value=purchase_value,
        useful_life_hours=useful_life_hours,
        energy_watts=energy_watts,
        kwh_cost=kwh_cost,
        monthly_maintenance_cost=monthly_maintenance_cost,
        monthly_fixed_cost=float(settings["monthly_fixed_cost"] or 0),
        productive_hours_per_month=float(settings["productive_hours_per_month"] or 0),
    )
    return {
        "name": request.form["name"].strip(),
        "brand": request.form["brand"].strip(),
        "model": request.form["model"].strip(),
        "serial_number": request.form["serial_number"].strip(),
        "technology": request.form["technology"].strip(),
        "nozzle_size": (
            float(request.form["nozzle_size"]) if request.form["nozzle_size"] else None
        ),
        "build_volume": request.form["build_volume"].strip(),
        "location": request.form.get("location", "").strip(),
        "status": request.form["status"].strip(),
        "purchase_date": request.form["purchase_date"] or None,
        "last_maintenance_date": request.form.get("last_maintenance_date") or None,
        "next_maintenance_date": request.form.get("next_maintenance_date") or None,
        "hourly_cost": hourly_cost,
        "energy_watts": energy_watts,
        "purchase_value": purchase_value,
        "useful_life_hours": useful_life_hours,
        "monthly_maintenance_cost": monthly_maintenance_cost,
        "has_ams": 1 if request.form.get("has_ams") == "on" else 0,
        "ams_model": request.form["ams_model"].strip(),
        "kwh_cost": kwh_cost,
        "notes": request.form["notes"].strip(),
    }


def build_filament_dryer_form_data() -> dict[str, Any]:
    price = parse_brazilian_decimal(request.form.get("price"))
    useful_life_hours = float(request.form["useful_life_hours"] or 0)
    power_watts = float(request.form["power_watts"] or 0)
    kwh_cost = parse_brazilian_decimal(request.form.get("kwh_cost"))
    hourly_cost = calculate_printer_hourly_cost(
        purchase_value=price,
        useful_life_hours=useful_life_hours,
        energy_watts=power_watts,
        kwh_cost=kwh_cost,
    )
    return {
        "brand": request.form["brand"].strip(),
        "model": request.form["model"].strip(),
        "dryer_type": request.form["dryer_type"].strip(),
        "power_watts": power_watts,
        "useful_life_hours": useful_life_hours,
        "price": price,
        "kwh_cost": kwh_cost,
        "hourly_cost": hourly_cost,
    }


def build_component_form_data() -> dict[str, Any]:
    stock_quantity = float(request.form.get("stock_quantity") or 0)
    product_cost = parse_brazilian_decimal(request.form.get("product_cost"))
    shipping_cost = 0.0
    store_discount = 0.0
    coupon_discount = 0.0
    payment_discount = 0.0
    real_total_cost, unit_cost = calculate_component_costs(
        stock_quantity=stock_quantity,
        product_cost=product_cost,
        shipping_cost=shipping_cost,
        store_discount=store_discount,
        coupon_discount=coupon_discount,
        payment_discount=payment_discount,
    )
    return {
        "name": request.form.get("name", "").strip(),
        "component_type": request.form.get("component_type", "").strip(),
        "manufacturer_name": request.form.get("manufacturer_name", "").strip(),
        "part_number": request.form.get("part_number", "").strip(),
        "location": request.form.get("location", "").strip(),
        "unit_cost": unit_cost,
        "product_cost": product_cost,
        "shipping_cost": shipping_cost,
        "store_discount": store_discount,
        "coupon_discount": coupon_discount,
        "payment_discount": payment_discount,
        "real_total_cost": real_total_cost,
        "unit_measure": request.form.get("unit_measure", "").strip(),
        "stock_quantity": stock_quantity,
        "minimum_quantity": float(request.form.get("minimum_quantity") or 0),
        "purchase_link": request.form.get("purchase_link", "").strip(),
        "notes": request.form.get("notes", "").strip(),
    }


def build_product_form_data(
    db: sqlite3.Connection, existing_product: sqlite3.Row | None = None
) -> dict[str, Any]:
    materials_by_id = {
        row["id"]: row
        for row in db.execute(
            f"SELECT * FROM materials ORDER BY {material_order_clause()}"
        ).fetchall()
    }
    components_by_id = {
        row["id"]: row
        for row in db.execute("SELECT * FROM components ORDER BY name ASC").fetchall()
    }

    product_material_lines: list[dict[str, Any]] = []
    detailed_material_lines: list[dict[str, Any]] = []
    material_ids = get_form_list("product_material_id")
    material_quantities = get_form_list("product_material_quantity")
    material_print_hours = get_form_list("product_material_print_hours")
    for index, raw_material_id in enumerate(material_ids):
        material_id = parse_integerish(raw_material_id or "")
        if material_id <= 0:
            continue
        material = materials_by_id.get(material_id)
        if material is None:
            continue
        quantity_grams = (
            parse_loose_float(material_quantities[index], 0.0)
            if index < len(material_quantities)
            else 0.0
        )
        line_print_hours = (
            parse_loose_float(material_print_hours[index], 0.0)
            if index < len(material_print_hours)
            else 0.0
        )
        product_material_lines.append(
            {
                "material_id": material_id,
                "label": build_product_material_label(material),
                "quantity_grams": quantity_grams,
                "print_hours": line_print_hours,
            }
        )
        detailed_material_lines.append(
            {
                "weight_grams": quantity_grams,
                "cost_per_kg": float(material["cost_per_kg"] or 0),
            }
        )

    product_component_lines: list[dict[str, Any]] = []
    detailed_component_lines: list[dict[str, Any]] = []
    component_ids = get_form_list("product_component_id")
    component_quantities = get_form_list("product_component_quantity")
    for index, raw_component_id in enumerate(component_ids):
        component_id = parse_integerish(raw_component_id or "")
        if component_id <= 0:
            continue
        component = components_by_id.get(component_id)
        if component is None:
            continue
        quantity = (
            parse_loose_float(component_quantities[index], 0.0)
            if index < len(component_quantities)
            else 0.0
        )
        product_component_lines.append(
            {
                "component_id": component_id,
                "label": build_product_component_label(component),
                "quantity": quantity,
            }
        )
        detailed_component_lines.append(
            {
                "quantity": quantity,
                "unit_cost": float(component["unit_cost"] or 0),
            }
        )

    material_id = product_material_lines[0]["material_id"] if product_material_lines else None
    weight_grams = round(
        sum(float(line["quantity_grams"]) for line in product_material_lines), 2
    )
    print_hours = round(
        sum(float(line["print_hours"]) for line in product_material_lines), 2
    )
    energy_cost_per_hour = parse_brazilian_decimal(
        request.form.get("energy_cost_per_hour")
    )
    operating_cost_per_hour = parse_brazilian_decimal(
        request.form.get("operating_cost_per_hour")
    )
    labor_hours = parse_loose_float(request.form.get("labor_hours"), 0.0)
    labor_hourly_rate = 0.0
    design_hours = (
        parse_loose_float(request.form.get("design_hours"), 0.0)
        if "design_hours" in request.form
        else parse_loose_float(existing_product["design_hours"], 0.0)
        if existing_product
        else 0.0
    )
    design_hourly_rate = parse_brazilian_decimal(request.form.get("design_hourly_rate"))
    extra_cost = parse_brazilian_decimal(request.form.get("extra_cost"))
    margin_percent = parse_loose_float(request.form.get("margin_percent"), 0.0)
    unit_cost, calculated_sale_price = calculate_detailed_job_values(
        material_lines=detailed_material_lines,
        component_lines=detailed_component_lines,
        print_hours=print_hours,
        energy_cost_per_hour=energy_cost_per_hour,
        operating_cost_per_hour=0,
        dryer_hours=0,
        dryer_cost_per_hour=0,
        labor_hours=labor_hours,
        labor_hourly_rate=operating_cost_per_hour,
        design_hours=design_hours,
        design_hourly_rate=design_hourly_rate,
        extra_cost=extra_cost,
        margin_percent=margin_percent,
    )
    sale_price = parse_brazilian_decimal(request.form.get("sale_price"))
    if sale_price <= 0:
        sale_price = calculated_sale_price

    return {
        "name": request.form.get("name", "").strip(),
        "category": request.form.get("category", "").strip(),
        "description": request.form.get("description", "").strip(),
        "material_id": material_id,
        "weight_grams": weight_grams,
        "print_hours": print_hours,
        "energy_cost_per_hour": energy_cost_per_hour,
        "operating_cost_per_hour": operating_cost_per_hour,
        "labor_hours": labor_hours,
        "labor_hourly_rate": labor_hourly_rate,
        "design_hours": design_hours,
        "design_hourly_rate": design_hourly_rate,
        "extra_cost": extra_cost,
        "margin_percent": margin_percent,
        "unit_cost": unit_cost,
        "sale_price": sale_price,
        "stock_quantity": parse_loose_float(request.form.get("stock_quantity"), 0.0),
        "minimum_quantity": parse_loose_float(request.form.get("minimum_quantity"), 0.0),
        "sale_channel": (
            request.form.get("sale_channel", "").strip()
            if "sale_channel" in request.form
            else existing_product["sale_channel"] if existing_product else ""
        ),
        "status": request.form.get("status", "").strip(),
        "model_link": request.form.get("model_link", "").strip(),
        "notes": request.form.get("notes", "").strip(),
        "additional_material_types": json.dumps(product_material_lines, ensure_ascii=True),
        "accessories": json.dumps(product_component_lines, ensure_ascii=True),
    }


def get_registry_page_context(
    section: str,
    references: dict[str, list[sqlite3.Row]],
    db: sqlite3.Connection | None = None,
) -> dict[str, Any] | None:
    active_db = db or get_db()
    operational_settings = get_operational_cost_settings(active_db)
    productive_hours_per_month = float(
        operational_settings["productive_hours_per_month"] or 0
    )
    monthly_fixed_cost = float(operational_settings["monthly_fixed_cost"] or 0)
    shared_operating_hourly_cost = calculate_shared_operating_hourly_cost(
        monthly_fixed_cost, productive_hours_per_month
    )
    page_map: dict[str, dict[str, Any]] = {
        "customers": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de clientes",
            "description": "Mantenha sua base comercial organizada para transformar atendimentos em pedidos.",
            "panel_kicker": "Rede comercial",
            "panel_title": "Clientes",
            "panel_badge": f"{len(references['customers'])} cadastrados",
            "submit_label": "Salvar cliente",
            "form_class": "customer-registry-form",
            "fields": [
                {"name": "name", "label": "Nome", "type": "text", "required": True, "class": "auto-uppercase"},
                {"name": "customer_type", "label": "Tipo de cliente", "type": "select", "options": ["CONSUMIDOR", "REVENDA"], "class": "auto-uppercase"},
                {"name": "document", "label": "Documento", "type": "text", "placeholder": "CPF ou CNPJ", "class": "auto-uppercase"},
                {"name": "phone", "label": "Telefone", "type": "text"},
                {"name": "email", "label": "Email", "type": "email"},
                {"name": "lead_source", "label": "Como conheceu nossa loja", "type": "text", "placeholder": "Instagram, indicação, Google...", "class": "auto-uppercase"},
                {"name": "postal_code", "label": "CEP", "type": "text", "placeholder": "00000-000", "class": "customer-postal-code", "inputmode": "numeric"},
                {"name": "street", "label": "Rua / Endereço", "type": "text", "placeholder": "Nome da rua", "full": True, "class": "customer-street auto-uppercase"},
                {"name": "address_number", "label": "Número", "type": "text", "placeholder": "S/N", "class": "customer-address-number auto-uppercase"},
                {"name": "address_complement", "label": "Complemento", "type": "text", "placeholder": "Apto, sala, bloco...", "class": "customer-address-complement auto-uppercase"},
                {"name": "neighborhood", "label": "Bairro", "type": "text", "placeholder": "Centro", "class": "customer-neighborhood auto-uppercase"},
                {"name": "city", "label": "Cidade", "type": "text", "placeholder": "Cidade", "class": "customer-city auto-uppercase"},
                {"name": "state", "label": "Estado", "type": "text", "placeholder": "UF", "class": "customer-state auto-uppercase"},
                {"name": "segment", "label": "Segmento / Interesse", "type": "text", "placeholder": "Arquitetura, brindes, games...", "class": "auto-uppercase"},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True, "class": "auto-uppercase"},
            ],
            "columns": [
                {"key": "name", "label": "Nome"},
                {"key": "customer_type", "label": "Tipo"},
                {"key": "contact", "label": "Contato"},
                {"key": "city", "label": "Cidade"},
                {"key": "lead_source", "label": "Origem"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "customer_type": row["customer_type"] or "-",
                    "contact": row["phone"] or row["email"] or "-",
                    "city": row["city"] or "-",
                    "lead_source": row["lead_source"] or "-",
                }
                for row in references["customers"]
            ],
            "list_class": "customer-records-panel",
            "table_class": "customer-records-table",
        },
        "suppliers": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de fornecedores",
            "description": "Centralize contatos e prazos de compra para manter a reposicao previsivel.",
            "panel_kicker": "Compras",
            "panel_title": "Fornecedores",
            "panel_badge": f"{len(references['suppliers'])} cadastrados",
            "submit_label": "Salvar fornecedor",
            "fields": [
                {"name": "name", "label": "Empresa", "type": "text", "required": True},
                {"name": "contact_name", "label": "Contato", "type": "text"},
                {"name": "phone", "label": "Telefone", "type": "text"},
                {"name": "email", "label": "Email", "type": "email"},
                {"name": "supplier_link", "label": "Link do fornecedor", "type": "text"},
                {"name": "lead_time_days", "label": "Lead time (dias)", "type": "number", "min": "0", "step": "1", "value": "0"},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Empresa"},
                {"key": "contact_name", "label": "Contato"},
                {"key": "phone", "label": "Telefone"},
                {"key": "supplier_link", "label": "Link"},
                {"key": "lead_time_days", "label": "Lead time"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "contact_name": row["contact_name"] or "-",
                    "phone": row["phone"] or row["email"] or "-",
                    "supplier_link": row["supplier_link"] or "-",
                    "lead_time_days": f"{row['lead_time_days']} dias",
                }
                for row in references["suppliers"]
            ],
        },
        "representatives": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de representantes",
            "description": "Acompanhe quem traz vendas, acordos comerciais e comissoes combinadas.",
            "panel_kicker": "Canal de vendas",
            "panel_title": "Representantes",
            "panel_badge": f"{len(references['representatives'])} cadastrados",
            "submit_label": "Salvar representante",
            "fields": [
                {"name": "name", "label": "Nome", "type": "text", "required": True},
                {"name": "phone", "label": "Telefone", "type": "text"},
                {"name": "email", "label": "Email", "type": "email"},
                {"name": "commission_percent", "label": "Comissao (%)", "type": "number", "min": "0", "step": "0.01", "value": "0"},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Nome"},
                {"key": "phone", "label": "Telefone"},
                {"key": "email", "label": "Email"},
                {"key": "commission_percent", "label": "Comissao"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "phone": row["phone"] or "-",
                    "email": row["email"] or "-",
                    "commission_percent": f"{row['commission_percent']:.2f}%",
                }
                for row in references["representatives"]
            ],
        },
        "partner-stores": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de lojas parceiras",
            "description": "Organize os pontos de venda e canais parceiros que ajudam na distribuicao.",
            "panel_kicker": "Canais parceiros",
            "panel_title": "Lojas parceiras",
            "panel_badge": f"{len(references['partner_stores'])} cadastradas",
            "submit_label": "Salvar loja",
            "fields": [
                {"name": "name", "label": "Nome", "type": "text", "required": True},
                {"name": "city", "label": "Cidade", "type": "text"},
                {"name": "contact_name", "label": "Contato", "type": "text"},
                {"name": "phone", "label": "Telefone", "type": "text"},
                {"name": "instagram", "label": "Instagram", "type": "text"},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Nome"},
                {"key": "city", "label": "Cidade"},
                {"key": "contact_name", "label": "Contato"},
                {"key": "phone", "label": "Telefone"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "city": row["city"] or "-",
                    "contact_name": row["contact_name"] or "-",
                    "phone": row["phone"] or row["instagram"] or "-",
                }
                for row in references["partner_stores"]
            ],
        },
        "payment-terms": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de condições de pagamento",
            "description": "Padronize as condições comerciais usadas nos pedidos e orçamentos.",
            "panel_kicker": "Comercial",
            "panel_title": "Condicoes de pagamento",
            "panel_badge": f"{len(references['payment_terms'])} cadastradas",
            "submit_label": "Salvar condicao",
            "fields": [
                {"name": "name", "label": "Condicao de pagamento", "type": "text", "required": True, "placeholder": "Pix a vista, 50% entrada e 50% na entrega..."},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Condicao"},
                {"key": "notes", "label": "Observações"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "notes": row["notes"] or "-",
                }
                for row in references["payment_terms"]
            ],
        },
        "sales-channels": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de canais de venda",
            "description": "Padronize os canais comerciais usados para captar e fechar pedidos.",
            "panel_kicker": "Comercial",
            "panel_title": "Canais de venda",
            "panel_badge": f"{len(references['sales_channels'])} cadastrados",
            "submit_label": "Salvar canal",
            "fields": [
                {"name": "name", "label": "Canal de venda", "type": "text", "required": True, "placeholder": "Instagram, WhatsApp, Loja fisica..."},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Canal"},
                {"key": "notes", "label": "Observações"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "notes": row["notes"] or "-",
                }
                for row in references["sales_channels"]
            ],
        },
        "material-types": {
            "eyebrow": "Cadastros",
            "title": "Cadastro de tipos de filamento",
            "description": "Padronize os tipos base do catálogo técnico, como PLA, PETG, ABS e resina.",
            "panel_kicker": "Estrutura produtiva",
            "panel_title": "Tipos de filamento",
            "panel_badge": f"{len(references['material_types'])} cadastrados",
            "submit_label": "Salvar tipo",
            "fields": [
                {"name": "name", "label": "Tipo de filamento", "type": "text", "required": True, "placeholder": "PLA, PETG, ABS, Resina..."},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Tipo"},
                {"key": "notes", "label": "Observações"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "notes": row["notes"] or "-",
                }
                for row in references["material_types"]
            ],
        },
        "operational-costs": {
            "eyebrow": "Estrutura produtiva",
            "title": "Base de custos operacionais",
            "description": "Defina os custos fixos mensais e a capacidade produtiva para calcular automaticamente o operacional por hora.",
            "panel_kicker": "Custos",
            "panel_title": "Custos operacionais",
            "panel_badge": f"R$ {br_money(shared_operating_hourly_cost)}/h",
            "submit_label": "Salvar custos operacionais",
            "form_class": "material-form operational-cost-form",
            "hide_records": True,
            "fields": [
                {
                    "name": "monthly_fixed_cost",
                    "label": "Custos fixos mensais (R$)",
                    "type": "text",
                    "class": "currency-field",
                    "inputmode": "decimal",
                    "value": br_money(monthly_fixed_cost),
                    "label_class": "span-3",
                },
                {
                    "name": "productive_hours_per_month",
                    "label": "Horas produtivas por mês",
                    "type": "number",
                    "min": "0",
                    "step": "0.01",
                    "value": productive_hours_per_month,
                    "label_class": "span-3",
                },
                {
                    "name": "shared_overhead_hourly_cost",
                    "label": "Operacional base por hora",
                    "type": "text",
                    "value": br_money(shared_operating_hourly_cost),
                    "readonly": True,
                    "label_class": "span-3",
                },
                {
                    "name": "notes",
                    "label": "Observações",
                    "type": "textarea",
                    "full": True,
                    "value": operational_settings["notes"] or "",
                    "placeholder": "Aluguel, internet, bancada, limpeza, softwares, equipe indireta...",
                },
            ],
            "columns": [],
            "records": [],
        },
        "printers": {
            "eyebrow": "Estrutura produtiva",
            "title": "Cadastro de impressoras",
            "description": "Monte sua fazenda de impressão com dados técnicos e status operacional.",
            "panel_kicker": "Maquinas",
            "panel_title": "Impressoras",
            "panel_badge": f"{len(references['printers'])} cadastradas",
            "submit_label": "Salvar impressora",
            "form_class": "material-form",
            "fields": [
                {"name": "name", "label": "Nome", "type": "text", "placeholder": "Farm 01", "required": True, "label_class": "span-3"},
                {"name": "brand", "label": "Marca", "type": "text", "placeholder": "Bambu Lab", "label_class": "span-2"},
                {"name": "model", "label": "Modelo", "type": "text", "placeholder": "P1S, A1...", "label_class": "span-2"},
                {"name": "has_ams", "label": "Tem AMS", "type": "checkbox", "label_class": "span-2"},
                {"name": "ams_model", "label": "Modelo do AMS", "type": "text", "placeholder": "AMS, AMS Lite...", "label_class": "span-3"},
                {"name": "serial_number", "label": "Numero de serie", "type": "text", "label_class": "span-2"},
                {"name": "technology", "label": "Tecnologia", "type": "text", "placeholder": "FDM, SLA, MSLA...", "label_class": "span-2"},
                {"name": "nozzle_size", "label": "Bico (mm)", "type": "number", "min": "0", "step": "0.01", "placeholder": "0.4", "label_class": "span-2"},
                {"name": "build_volume", "label": "Volume util", "type": "text", "placeholder": "220x220x250 mm", "label_class": "span-3"},
                {"name": "status", "label": "Status", "type": "select", "options": PRINTER_STATUSES, "value": "Operando", "label_class": "span-3"},
                {"name": "purchase_date", "label": "Data da compra", "type": "date", "label_class": "span-3"},
                {"name": "purchase_value", "label": "Valor da impressora", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-3"},
                {"name": "energy_watts", "label": "Potencia (W)", "type": "number", "min": "0", "step": "0.01", "value": "0", "label_class": "span-3"},
                {"name": "useful_life_hours", "label": "Vida util (horas)", "type": "number", "min": "0", "step": "1", "value": "0", "label_class": "span-3"},
                {"name": "depreciation_hourly_cost", "label": "Depreciacao/hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-3"},
                {"name": "monthly_maintenance_cost", "label": "Manutencao mensal (R$)", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-3"},
                {"name": "maintenance_hourly_cost", "label": "Manutencao/hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-3"},
                {"name": "kwh_cost", "label": "Valor kWh", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-3"},
                {"name": "energy_hourly_cost", "label": "Energia/hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-3"},
                {"name": "shared_overhead_hourly_cost", "label": "Rateio operacional/h", "type": "text", "inputmode": "decimal", "value": br_money(shared_operating_hourly_cost), "readonly": True, "label_class": "span-3"},
                {"name": "operating_hourly_cost", "label": "Operacional/h", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-3"},
                {"name": "hourly_cost", "label": "Custo total da impressora por hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-3"},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "name", "label": "Nome"},
                {"key": "technology", "label": "Tecnologia"},
                {"key": "model", "label": "Modelo"},
                {"key": "nozzle_size", "label": "Bico"},
                {"key": "purchase_value", "label": "Valor"},
                {"key": "useful_life", "label": "Vida util"},
                {"key": "energy", "label": "Energia"},
                {"key": "ams", "label": "AMS"},
                {"key": "maintenance", "label": "Manutencao"},
                {"key": "cost", "label": "Custo"},
                {"key": "status", "label": "Status"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "technology": row["technology"] or "-",
                    "model": " ".join(
                        value for value in [row["brand"], row["model"]] if value
                    )
                    or "-",
                    "nozzle_size": f"{row['nozzle_size']:.2f} mm" if row["nozzle_size"] is not None else "-",
                    "purchase_value": f"R$ {br_money(row['purchase_value'])}",
                    "useful_life": f"{row['useful_life_hours']:.0f} h",
                    "energy": f"{row['energy_watts']:.0f} W · R$ {br_money(row['kwh_cost'])}/kWh",
                    "ams": row["ams_model"] or "Sim" if row["has_ams"] else "Nao",
                    "maintenance": row["next_maintenance_date"] or "-",
                    "cost": f"R$ {br_money(row['hourly_cost'])}/h",
                    "status": row["status"] or "-",
                    "action_url": url_for("edit_printer", printer_id=row["id"]),
                }
                for row in references["printers"]
            ],
        },
        "components": {
            "eyebrow": "Estrutura produtiva",
            "title": "Cadastro de componentes",
            "description": "Controle pecas, reposicoes e componentes usados na bancada e nas impressoras.",
            "panel_kicker": "Suprimentos",
            "panel_title": "Componentes",
            "panel_badge": f"{len(references['components'])} cadastrados",
            "submit_label": "Salvar componente",
            "form_class": "material-form component-form",
            "fields": [
                {"name": "sku", "label": "Codigo interno", "type": "text", "value": generate_sequential_code(get_db(), "components", "sku", "C"), "readonly": True, "label_class": "span-2"},
                {"name": "name", "label": "Nome", "type": "text", "placeholder": "Bico 0.4, hotend, correia GT2...", "required": True, "label_class": "span-5"},
                {"name": "component_type", "label": "Tipo", "type": "text", "placeholder": "Bico, hotend, extrusor, sensor...", "label_class": "span-3"},
                {"name": "manufacturer_name", "label": "Fabricante", "type": "text", "placeholder": "Bambu Lab, Creality, Generico...", "label_class": "span-3"},
                {"name": "part_number", "label": "Modelo/codigo da peca", "type": "text", "placeholder": "A1-P1, MK8, GT2...", "label_class": "span-2"},
                {"name": "unit_measure", "label": "Unidade de medida", "type": "text", "placeholder": "un, kit, m, par...", "label_class": "span-3"},
                {"name": "product_cost", "label": "Valor medio do material", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-3"},
                {"name": "stock_quantity", "label": "Estoque", "type": "number", "min": "0", "step": "0.01", "value": "0", "readonly": True, "label_class": "span-3"},
                {"name": "minimum_quantity", "label": "Minimo", "type": "number", "min": "0", "step": "0.01", "value": "0", "label_class": "span-3"},
                {"name": "location", "label": "Localizacao", "type": "text", "placeholder": "Gaveta B2", "label_class": "span-3"},
                {"name": "purchase_link", "label": "Link de compra", "type": "text", "placeholder": "www.fornecedor.com/produto", "full": True},
                {"name": "notes", "label": "Observações", "type": "textarea", "full": True},
            ],
            "columns": [
                {"key": "sku", "label": "Codigo"},
                {"key": "name", "label": "Nome"},
                {"key": "component_type", "label": "Tipo"},
                {"key": "manufacturer_name", "label": "Fabricante"},
                {"key": "location", "label": "Local"},
                {"key": "real_total_cost", "label": "Total real"},
                {"key": "unit_cost", "label": "Custo unit."},
                {"key": "stock_quantity", "label": "Estoque"},
                {"key": "minimum_quantity", "label": "Minimo"},
                {"key": "unit_measure", "label": "Unidade"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "sku": row["sku"] or "-",
                    "name": row["name"],
                    "component_type": row["component_type"] or "-",
                    "manufacturer_name": row["manufacturer_name"] or "-",
                    "location": row["location"] or "-",
                    "real_total_cost": f"R$ {br_money(row['real_total_cost'])}",
                    "unit_cost": f"R$ {br_decimal(row['unit_cost'], 4)}",
                    "stock_quantity": f"{row['stock_quantity']:.2f}",
                    "minimum_quantity": f"{row['minimum_quantity']:.2f}",
                    "unit_measure": row["unit_measure"] or "-",
                    "action_url": url_for("edit_component", component_id=row["id"]),
                }
                for row in references["components"]
            ],
        },
        "filament-dryers": {
            "eyebrow": "Estrutura produtiva",
            "title": "Cadastro de secador de filamentos",
            "description": "Cadastre secadores usados para manter filamentos secos antes da impressão.",
            "panel_kicker": "Secador",
            "panel_title": "Secador de filamentos",
            "panel_badge": f"{len(references['filament_dryers'])} cadastrados",
            "submit_label": "Salvar secador",
            "form_class": "material-form",
            "fields": [
                {"name": "brand", "label": "Marca", "type": "text", "placeholder": "Sovol", "required": True, "label_class": "span-2"},
                {"name": "model", "label": "Modelo", "type": "text", "placeholder": "SH01", "required": True, "label_class": "span-2"},
                {"name": "dryer_type", "label": "Tipo", "type": "text", "placeholder": "Carretel duplo", "label_class": "span-2"},
                {"name": "power_watts", "label": "Potencia (Watts)", "type": "number", "min": "0", "step": "0.01", "value": "0", "label_class": "span-2"},
                {"name": "useful_life_hours", "label": "Horas de vida util", "type": "number", "min": "0", "step": "1", "value": "0", "label_class": "span-2"},
                {"name": "price", "label": "Preco (R$)", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-2"},
                {"name": "kwh_cost", "label": "Valor kWh", "type": "text", "class": "currency-field", "inputmode": "decimal", "value": "0,00", "label_class": "span-2"},
                {"name": "depreciation_hourly_cost", "label": "Depreciacao/hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-2"},
                {"name": "hourly_cost", "label": "Custo R$/hora", "type": "text", "inputmode": "decimal", "value": "0,00", "readonly": True, "label_class": "span-2"},
            ],
            "columns": [
                {"key": "brand", "label": "Marca"},
                {"key": "model", "label": "Modelo"},
                {"key": "dryer_type", "label": "Tipo"},
                {"key": "power_watts", "label": "Potencia"},
                {"key": "useful_life_hours", "label": "Vida util"},
                {"key": "price", "label": "Preco"},
                {"key": "kwh_cost", "label": "kWh"},
                {"key": "hourly_cost", "label": "Custo/hora"},
            ],
            "records": [
                {
                    "id": row["id"],
                    "brand": row["brand"],
                    "model": row["model"],
                    "dryer_type": row["dryer_type"] or "-",
                    "power_watts": f"{row['power_watts']:.0f} W",
                    "useful_life_hours": f"{row['useful_life_hours']:.0f} h",
                    "price": f"R$ {br_money(row['price'])}",
                    "kwh_cost": f"R$ {br_money(row['kwh_cost'])}",
                    "hourly_cost": f"R$ {br_money(row['hourly_cost'])}/h",
                    "action_url": url_for("edit_filament_dryer", dryer_id=row["id"]),
                }
                for row in references["filament_dryers"]
            ],
        },
    }
    page = page_map.get(section)
    if page is not None:
        page["records"] = apply_record_actions(section, page["records"])
    return page


SIMPLE_REGISTRY_TABLES = {
    "customers": "customers",
    "suppliers": "suppliers",
    "representatives": "representatives",
    "partner-stores": "partner_stores",
    "payment-terms": "payment_terms",
    "sales-channels": "sales_channels",
    "material-types": "material_types",
}

REGISTRY_DELETE_TABLES = {
    **SIMPLE_REGISTRY_TABLES,
    "printers": "printers",
    "filament-dryers": "filament_dryers",
    "components": "components",
}


def apply_record_actions(
    section: str, records: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    for record in records:
        record_id = record.get("id")
        if record_id is None:
            continue
        if "action_url" not in record:
            record["action_url"] = url_for(
                "edit_registry_item",
                section=section,
                record_id=record_id,
            )
        record["delete_url"] = url_for(
            "delete_registry_item",
            section=section,
            record_id=record_id,
        )
    return records


def with_registry_record_values(
    fields: list[dict[str, Any]], record: sqlite3.Row
) -> list[dict[str, Any]]:
    valued_fields = []
    for field in fields:
        valued_field = dict(field)
        field_name = valued_field["name"]
        if field_name in record.keys():
            valued_field["value"] = record[field_name] or ""
        valued_fields.append(valued_field)
    return valued_fields


def handle_registry_update(
    db: sqlite3.Connection, section: str, record_id: int
) -> None:
    if section == "customers":
        db.execute(
            """
            UPDATE customers
            SET
                name = ?,
                document = ?,
                phone = ?,
                email = ?,
                customer_type = ?,
                postal_code = ?,
                street = ?,
                address_number = ?,
                address_complement = ?,
                neighborhood = ?,
                city = ?,
                state = ?,
                lead_source = ?,
                segment = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                normalize_upper_text(request.form["name"]),
                normalize_upper_text(request.form["document"]),
                request.form["phone"].strip(),
                request.form["email"].strip().lower(),
                normalize_upper_text(request.form.get("customer_type", "")),
                request.form.get("postal_code", "").strip(),
                normalize_upper_text(request.form.get("street", "")),
                normalize_upper_text(request.form.get("address_number", "")),
                normalize_upper_text(request.form.get("address_complement", "")),
                normalize_upper_text(request.form.get("neighborhood", "")),
                normalize_upper_text(request.form["city"]),
                normalize_upper_text(request.form.get("state", "")),
                normalize_upper_text(request.form.get("lead_source", "")),
                normalize_upper_text(request.form.get("segment", "")),
                normalize_upper_text(request.form["notes"]),
                record_id,
            ),
        )
    elif section == "suppliers":
        db.execute(
            """
            UPDATE suppliers
            SET name = ?, contact_name = ?, phone = ?, email = ?, supplier_link = ?, lead_time_days = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["contact_name"].strip(),
                request.form["phone"].strip(),
                request.form["email"].strip(),
                request.form.get("supplier_link", "").strip(),
                int(request.form["lead_time_days"] or 0),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    elif section == "representatives":
        db.execute(
            """
            UPDATE representatives
            SET name = ?, phone = ?, email = ?, commission_percent = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["phone"].strip(),
                request.form["email"].strip(),
                float(request.form["commission_percent"] or 0),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    elif section == "partner-stores":
        db.execute(
            """
            UPDATE partner_stores
            SET name = ?, city = ?, contact_name = ?, phone = ?, instagram = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["city"].strip(),
                request.form["contact_name"].strip(),
                request.form["phone"].strip(),
                request.form["instagram"].strip(),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    elif section == "payment-terms":
        db.execute(
            """
            UPDATE payment_terms
            SET name = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    elif section == "sales-channels":
        db.execute(
            """
            UPDATE sales_channels
            SET name = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    elif section == "material-types":
        db.execute(
            """
            UPDATE material_types
            SET name = ?, notes = ?
            WHERE id = ?
            """,
            (
                request.form["name"].strip(),
                request.form["notes"].strip(),
                record_id,
            ),
        )
    else:
        abort(404)
    db.commit()


def reference_count(
    db: sqlite3.Connection,
    table_name: str,
    column_name: str,
    record_id: int,
) -> int:
    row = db.execute(
        f"SELECT COUNT(*) AS total FROM {table_name} WHERE {column_name} = ?",
        (record_id,),
    ).fetchone()
    return int(row["total"])


def can_delete_registry_item(
    db: sqlite3.Connection, section: str, record_id: int
) -> tuple[bool, str | None]:
    if section == "components" and reference_count(db, "job_components", "component_id", record_id):
        return False, "Este componente esta ligado a pedido e nao foi excluido."
    return True, None


def prepare_registry_delete(db: sqlite3.Connection, section: str, record_id: int) -> None:
    if section == "customers":
        db.execute("UPDATE jobs SET customer_id = NULL WHERE customer_id = ?", (record_id,))
    elif section == "suppliers":
        db.execute("UPDATE materials SET supplier_id = NULL WHERE supplier_id = ?", (record_id,))
    elif section == "representatives":
        db.execute(
            "UPDATE jobs SET representative_id = NULL WHERE representative_id = ?",
            (record_id,),
        )
    elif section == "partner-stores":
        db.execute(
            "UPDATE jobs SET partner_store_id = NULL WHERE partner_store_id = ?",
            (record_id,),
        )
    elif section == "printers":
        db.execute("UPDATE jobs SET printer_id = NULL WHERE printer_id = ?", (record_id,))
    elif section == "filament-dryers":
        db.execute(
            "UPDATE jobs SET filament_dryer_id = NULL WHERE filament_dryer_id = ?",
            (record_id,),
        )


@app.route("/")
def dashboard() -> str:
    db = get_db()
    totals = db.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM customers) AS total_customers,
            (SELECT COUNT(*) FROM suppliers) AS total_suppliers,
            (SELECT COUNT(*) FROM materials) AS total_materials,
            (SELECT COUNT(*) FROM products) AS total_products,
            (SELECT COUNT(*) FROM jobs WHERE status NOT IN ('Entregue', 'Cancelado')) AS active_jobs,
            (SELECT COUNT(*) FROM jobs WHERE status = 'Orcamento') AS total_quotes,
            (SELECT COUNT(*) FROM jobs WHERE status = 'Pronto para entrega') AS ready_to_ship,
            (SELECT IFNULL(SUM(suggested_price), 0) FROM jobs WHERE status NOT IN ('Entregue', 'Cancelado')) AS pipeline_value,
            (SELECT IFNULL(SUM(suggested_price), 0) FROM jobs WHERE status = 'Entregue') AS delivered_revenue,
            (SELECT IFNULL(SUM(stock_grams), 0) FROM materials) AS total_stock,
            (SELECT COUNT(*) FROM materials WHERE stock_grams <= minimum_stock_grams) AS low_stock_count
        """
    ).fetchone()

    recent_jobs = db.execute(
        """
        SELECT
            jobs.*,
            materials.name AS material_name,
            materials.color AS material_color,
            customers.name AS linked_customer_name
        FROM jobs
        JOIN materials ON materials.id = jobs.material_id
        LEFT JOIN customers ON customers.id = jobs.customer_id
        ORDER BY jobs.created_at DESC, jobs.id DESC
        LIMIT 6
        """
    ).fetchall()

    low_stock = db.execute(
        """
        SELECT
            materials.*,
            suppliers.name AS supplier_name
        FROM materials
        LEFT JOIN suppliers ON suppliers.id = materials.supplier_id
        WHERE materials.stock_grams <= materials.minimum_stock_grams
        ORDER BY materials.stock_grams ASC, materials.name ASC
        LIMIT 6
        """
    ).fetchall()

    upcoming_jobs = db.execute(
        """
        SELECT
            jobs.id,
            jobs.item_name,
            jobs.status,
            jobs.due_date,
            COALESCE(customers.name, jobs.customer_name) AS customer_display
        FROM jobs
        LEFT JOIN customers ON customers.id = jobs.customer_id
        WHERE jobs.due_date IS NOT NULL
          AND jobs.status NOT IN ('Entregue', 'Cancelado')
        ORDER BY jobs.due_date ASC, jobs.id DESC
        LIMIT 6
        """
    ).fetchall()

    return render_template(
        "dashboard.html",
        totals=totals,
        recent_jobs=recent_jobs,
        low_stock=low_stock,
        upcoming_jobs=upcoming_jobs,
    )


@app.route("/commercial", methods=["GET", "POST"])
def commercial() -> str:
    db = get_db()
    references = fetch_reference_data(db)
    suppliers = references["suppliers"]
    item_options = fetch_commercial_item_options(db)
    selected_entry_id = parse_integerish(request.args.get("selected_entry_id"))
    editing_entry = (
        fetch_commercial_entry(db, selected_entry_id) if selected_entry_id else None
    )
    editing_entries = fetch_commercial_entry_group(db, editing_entry)
    editing_summary = commercial_group_summary(editing_entries)

    if request.method == "POST":
        entries_data = build_commercial_entries_from_form(item_options)
        if not entries_data:
            return render_template(
                "commercial.html",
                suppliers=suppliers,
                item_options=item_options,
                today_date=date.today().isoformat(),
                entries=fetch_recent_commercial_entries(db),
                editing_entry=editing_entry,
                editing_entries=editing_entries,
                editing_summary=editing_summary,
                form_action=(
                    url_for("update_commercial_entry", entry_id=editing_entry["id"])
                    if editing_entry
                    else url_for("commercial")
                ),
                submit_label="Salvar alterações" if editing_entry else "Salvar lançamento",
                error="Preencha pelo menos um produto com código e quantidade para registrar a nota fiscal.",
            )

        insert_commercial_entries(db, entries_data)
        db.commit()
        return redirect(url_for("commercial"))

    return render_template(
        "commercial.html",
        suppliers=suppliers,
        item_options=item_options,
        today_date=date.today().isoformat(),
        entries=fetch_recent_commercial_entries(db),
        editing_entry=editing_entry,
        editing_entries=editing_entries,
        editing_summary=editing_summary,
        form_action=(
            url_for("update_commercial_entry", entry_id=editing_entry["id"])
            if editing_entry
            else url_for("commercial")
        ),
        submit_label="Salvar alterações" if editing_entry else "Salvar lançamento",
        error=None,
    )


@app.route("/commercial/<int:entry_id>/edit")
def edit_commercial_entry(entry_id: int) -> str:
    return redirect(url_for("commercial", selected_entry_id=entry_id))


@app.route("/commercial/<int:entry_id>/update", methods=["POST"])
def update_commercial_entry(entry_id: int) -> str:
    db = get_db()
    existing_entry = fetch_commercial_entry(db, entry_id)
    if existing_entry is None:
        abort(404)

    item_options = fetch_commercial_item_options(db)
    existing_entries = fetch_commercial_entry_group(db, existing_entry)
    group_id = existing_entry["invoice_group_id"] or uuid.uuid4().hex
    entries_data = build_commercial_entries_from_form(item_options, group_id)
    if not entries_data:
        return redirect(url_for("commercial", selected_entry_id=entry_id))

    for entry in existing_entries:
        apply_commercial_entry_stock(db, entry, -1)
    if existing_entry["invoice_group_id"]:
        db.execute(
            "DELETE FROM commercial_entries WHERE invoice_group_id = ?",
            (existing_entry["invoice_group_id"],),
        )
    else:
        db.execute("DELETE FROM commercial_entries WHERE id = ?", (entry_id,))
    insert_commercial_entries(db, entries_data)
    db.commit()
    return redirect(url_for("commercial"))


@app.route("/commercial/<int:entry_id>/delete", methods=["POST"])
def delete_commercial_entry(entry_id: int) -> str:
    db = get_db()
    entry = fetch_commercial_entry(db, entry_id)
    if entry is None:
        abort(404)
    apply_commercial_entry_stock(db, entry, -1)
    db.execute("DELETE FROM commercial_entries WHERE id = ?", (entry_id,))
    db.commit()
    return redirect(url_for("commercial"))


@app.route("/crm")
def crm() -> str:
    db = get_db()
    references = fetch_reference_data(db)
    return render_template("crm.html", menu_groups=build_registry_menu(db, references))


@app.route("/uploads/jobs/<path:filename>")
def uploaded_job_file(filename: str) -> Any:
    return send_from_directory(UPLOAD_DIR, filename)


@app.route("/uploads/products/<path:filename>")
def uploaded_product_file(filename: str) -> Any:
    return send_from_directory(PRODUCT_UPLOAD_DIR, filename)


@app.route("/registry/<section>", methods=["GET", "POST"])
def registry_page(section: str) -> str:
    if section == "accessories":
        return redirect(url_for("registry_page", section="components"))

    db = get_db()
    references = fetch_reference_data(db)
    page = get_registry_page_context(section, references, db)
    if page is None:
        abort(404)

    if request.method == "POST":
        created_id = handle_registry_submission(db, section)
        return_to = request.values.get("return_to", "").strip()
        if return_to:
            if section == "suppliers" and created_id is not None:
                return redirect(
                    append_query_value(return_to, "selected_supplier_id", created_id)
                )
            if section == "customers" and created_id is not None:
                return redirect(
                    append_query_value(return_to, "selected_customer_id", created_id)
                )
            if section == "components" and created_id is not None:
                return redirect(
                    append_query_value(return_to, "selected_component_id", created_id)
                )
            if section == "payment-terms":
                created_name = request.form.get("name", "").strip()
                if created_name:
                    return redirect(
                        append_query_value(
                            return_to, "selected_payment_term", created_name
                        )
                    )
            if section == "sales-channels":
                created_name = request.form.get("name", "").strip()
                if created_name:
                    return redirect(
                        append_query_value(
                            return_to, "selected_sale_channel", created_name
                        )
                    )
            if section == "material-types":
                created_name = request.form.get("name", "").strip()
                if created_name:
                    return redirect(
                        append_query_value(
                            return_to, "selected_material_type", created_name
                        )
                    )
            return redirect(return_to)
        return redirect(url_for("registry_page", section=section))

    return render_template(
        "registry.html",
        section=section,
        return_to=request.args.get("return_to", "").strip(),
        delete_error=request.args.get("delete_error", "").strip(),
        operational_settings=get_operational_cost_settings(db),
        **page,
    )


@app.route("/registry/<section>/<int:record_id>/edit", methods=["GET", "POST"])
def edit_registry_item(section: str, record_id: int) -> str:
    if section not in SIMPLE_REGISTRY_TABLES:
        abort(404)

    db = get_db()
    references = fetch_reference_data(db)
    page = get_registry_page_context(section, references, db)
    if page is None:
        abort(404)

    record = db.execute(
        f"SELECT * FROM {SIMPLE_REGISTRY_TABLES[section]} WHERE id = ?",
        (record_id,),
    ).fetchone()
    if record is None:
        abort(404)

    if request.method == "POST":
        handle_registry_update(db, section, record_id)
        return_to = request.values.get("return_to", "").strip()
        if return_to:
            return redirect(return_to)
        return redirect(url_for("registry_page", section=section))

    return render_template(
        "registry_edit.html",
        section=section,
        record_id=record_id,
        return_to=request.args.get("return_to", "").strip(),
        fields=with_registry_record_values(page["fields"], record),
        **{key: value for key, value in page.items() if key != "fields"},
    )


@app.route("/registry/<section>/<int:record_id>/delete", methods=["POST"])
def delete_registry_item(section: str, record_id: int) -> str:
    table_name = REGISTRY_DELETE_TABLES.get(section)
    if table_name is None:
        abort(404)

    db = get_db()
    can_delete, delete_error = can_delete_registry_item(db, section, record_id)
    if not can_delete:
        return redirect(
            url_for("registry_page", section=section, delete_error=delete_error)
        )

    prepare_registry_delete(db, section, record_id)
    db.execute(f"DELETE FROM {table_name} WHERE id = ?", (record_id,))
    db.commit()
    return redirect(url_for("registry_page", section=section))


@app.route("/printers/<int:printer_id>/edit", methods=["GET", "POST"])
def edit_printer(printer_id: int) -> str:
    db = get_db()
    printer = db.execute(
        """
        SELECT *
        FROM printers
        WHERE id = ?
        """,
        (printer_id,),
    ).fetchone()
    if printer is None:
        abort(404)

    if request.method == "POST":
        printer_data = build_printer_form_data(db)
        db.execute(
            """
            UPDATE printers
            SET
                name = ?,
                brand = ?,
                model = ?,
                serial_number = ?,
                technology = ?,
                nozzle_size = ?,
                build_volume = ?,
                location = ?,
                status = ?,
                purchase_date = ?,
                last_maintenance_date = ?,
                next_maintenance_date = ?,
                hourly_cost = ?,
                energy_watts = ?,
                purchase_value = ?,
                useful_life_hours = ?,
                monthly_maintenance_cost = ?,
                has_ams = ?,
                ams_model = ?,
                kwh_cost = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                printer_data["name"],
                printer_data["brand"],
                printer_data["model"],
                printer_data["serial_number"],
                printer_data["technology"],
                printer_data["nozzle_size"],
                printer_data["build_volume"],
                printer_data["location"],
                printer_data["status"],
                printer_data["purchase_date"],
                printer_data["last_maintenance_date"],
                printer_data["next_maintenance_date"],
                printer_data["hourly_cost"],
                printer_data["energy_watts"],
                printer_data["purchase_value"],
                printer_data["useful_life_hours"],
                printer_data["monthly_maintenance_cost"],
                printer_data["has_ams"],
                printer_data["ams_model"],
                printer_data["kwh_cost"],
                printer_data["notes"],
                printer_id,
            ),
        )
        db.commit()
        return redirect(url_for("registry_page", section="printers"))

    return render_template(
        "printer_edit.html",
        printer=printer,
        printer_statuses=PRINTER_STATUSES,
        operational_settings=get_operational_cost_settings(db),
    )


@app.route("/filament-dryers/<int:dryer_id>/edit", methods=["GET", "POST"])
def edit_filament_dryer(dryer_id: int) -> str:
    db = get_db()
    dryer = db.execute(
        """
        SELECT *
        FROM filament_dryers
        WHERE id = ?
        """,
        (dryer_id,),
    ).fetchone()
    if dryer is None:
        abort(404)

    if request.method == "POST":
        dryer_data = build_filament_dryer_form_data()
        db.execute(
            """
            UPDATE filament_dryers
            SET
                brand = ?,
                model = ?,
                dryer_type = ?,
                power_watts = ?,
                useful_life_hours = ?,
                price = ?,
                kwh_cost = ?,
                hourly_cost = ?,
                is_default = 0
            WHERE id = ?
            """,
            (
                dryer_data["brand"],
                dryer_data["model"],
                dryer_data["dryer_type"],
                dryer_data["power_watts"],
                dryer_data["useful_life_hours"],
                dryer_data["price"],
                dryer_data["kwh_cost"],
                dryer_data["hourly_cost"],
                dryer_id,
            ),
        )
        db.commit()
        return redirect(url_for("registry_page", section="filament-dryers"))

    return render_template("filament_dryer_edit.html", dryer=dryer)


@app.route("/components/<int:component_id>/edit", methods=["GET", "POST"])
def edit_component(component_id: int) -> str:
    db = get_db()
    component = db.execute(
        """
        SELECT *
        FROM components
        WHERE id = ?
        """,
        (component_id,),
    ).fetchone()
    if component is None:
        abort(404)

    if request.method == "POST":
        component_data = build_component_form_data()
        db.execute(
            """
            UPDATE components
            SET
                name = ?,
                component_type = ?,
                manufacturer_name = ?,
                part_number = ?,
                location = ?,
                unit_cost = ?,
                product_cost = ?,
                shipping_cost = ?,
                store_discount = ?,
                coupon_discount = ?,
                payment_discount = ?,
                real_total_cost = ?,
                unit_measure = ?,
                stock_quantity = ?,
                minimum_quantity = ?,
                purchase_link = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                component_data["name"],
                component_data["component_type"],
                component_data["manufacturer_name"],
                component_data["part_number"],
                component_data["location"],
                component_data["unit_cost"],
                component_data["product_cost"],
                component_data["shipping_cost"],
                component_data["store_discount"],
                component_data["coupon_discount"],
                component_data["payment_discount"],
                component_data["real_total_cost"],
                component_data["unit_measure"],
                component_data["stock_quantity"],
                component_data["minimum_quantity"],
                component_data["purchase_link"],
                component_data["notes"],
                component_id,
            ),
        )
        db.commit()
        return redirect(url_for("registry_page", section="components"))

    return render_template("component_edit.html", component=component)


@app.route("/materials", methods=["GET", "POST"])
def materials() -> str:
    db = get_db()
    references = fetch_reference_data(db)
    selected_material_type = request.args.get("selected_material_type", "").strip()
    selected_supplier_id = parse_integerish(request.args.get("selected_supplier_id"))
    return_to = request.values.get("return_to", "").strip()
    next_material_number = get_next_material_sequence_number(db)
    sort_key = request.args.get("sort", "name").strip()
    sort_direction = "desc" if request.args.get("direction", "").strip().lower() == "desc" else "asc"
    filters = {
        "sku": request.args.get("sku", "").strip(),
        "material_type": request.args.get("material_type", "").strip(),
        "color": request.args.get("color", "").strip(),
        "name": request.args.get("name", "").strip(),
        "manufacturer_name": request.args.get("manufacturer_name", "").strip(),
        "lot_number": request.args.get("lot_number", "").strip(),
        "location": request.args.get("location", "").strip(),
    }

    if request.method == "POST":
        stock_grams = float(request.form.get("stock_grams") or 0)
        product_cost = parse_brazilian_decimal(request.form.get("product_cost"))
        shipping_cost = parse_brazilian_decimal(request.form.get("shipping_cost"))
        store_discount = parse_brazilian_decimal(request.form.get("store_discount"))
        coupon_discount = parse_brazilian_decimal(request.form.get("coupon_discount"))
        payment_discount = parse_brazilian_decimal(request.form.get("payment_discount"))
        real_total_cost, cost_per_kg = calculate_material_costs(
            stock_grams=stock_grams,
            product_cost=product_cost,
            shipping_cost=shipping_cost,
            store_discount=store_discount,
            coupon_discount=coupon_discount,
            payment_discount=payment_discount,
        )
        material_code = build_material_code(
            request.form.get("material_type"),
            get_next_material_sequence_number(db),
        )

        fan_speed_min_percent = float(request.form.get("fan_speed_min_percent") or 0)
        fan_speed_max_percent = float(request.form.get("fan_speed_max_percent") or 0)
        flow_test_1_percent = float(request.form.get("flow_test_1_percent") or 0)
        flow_test_2_percent = float(request.form.get("flow_test_2_percent") or 0)
        cursor = db.execute(
            """
            INSERT INTO materials (
                name,
                material_type,
                line_series,
                color,
                color_hex,
                lot_number,
                stock_grams,
                cost_per_kg,
                supplier_id,
                sku,
                manufacturer_name,
                location,
                minimum_stock_grams,
                purchase_link,
                product_cost,
                shipping_cost,
                store_discount,
                coupon_discount,
                payment_discount,
                real_total_cost,
                nozzle_temperature_c,
                bed_temperature_c,
                fan_speed_percent,
                fan_speed_min_percent,
                fan_speed_max_percent,
                flow_percent,
                flow_test_1_percent,
                flow_test_2_percent,
                retraction_distance_mm,
                retraction_speed_mm_s,
                pressure_advance,
                print_speed_mm_s,
                xy_compensation_mm,
                humidity_percent,
                drying_required,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["line_series"].strip(),
                request.form["material_type"].strip(),
                request.form["line_series"].strip(),
                request.form["color"].strip(),
                request.form["color_hex"].strip(),
                request.form.get("lot_number", "").strip(),
                stock_grams,
                cost_per_kg,
                int(request.form["supplier_id"]) if request.form.get("supplier_id") else None,
                material_code,
                request.form["manufacturer_name"].strip(),
                request.form["location"].strip(),
                float(request.form["minimum_stock_grams"] or 0),
                request.form["purchase_link"].strip(),
                product_cost,
                shipping_cost,
                store_discount,
                coupon_discount,
                payment_discount,
                real_total_cost,
                float(request.form.get("nozzle_temperature_c") or 0),
                float(request.form.get("bed_temperature_c") or 0),
                fan_speed_max_percent,
                fan_speed_min_percent,
                fan_speed_max_percent,
                flow_test_1_percent,
                flow_test_1_percent,
                flow_test_2_percent,
                float(request.form.get("retraction_distance_mm") or 0),
                float(request.form.get("retraction_speed_mm_s") or 0),
                float(request.form.get("pressure_advance") or 0),
                float(request.form.get("print_speed_mm_s") or 0),
                float(request.form.get("xy_compensation_mm") or 0),
                float(request.form.get("humidity_percent") or 0),
                1 if request.form.get("drying_required") else 0,
                request.form["notes"].strip(),
            ),
        )
        created_id = cursor.lastrowid
        db.commit()
        if return_to:
            return redirect(append_query_value(return_to, "selected_material_id", created_id))
        return redirect(url_for("materials"))

    sort_columns = {
        "sku": "materials.sku",
        "material_type": "materials.material_type",
        "color": "materials.color",
        "name": "materials.name",
        "manufacturer_name": "materials.manufacturer_name",
        "lot_number": "materials.lot_number",
        "location": "materials.location",
    }
    order_column = sort_columns.get(sort_key, "materials.name")
    order_sql = f"""
        SELECT
            materials.*,
            suppliers.name AS supplier_name,
            ROUND(materials.cost_per_kg / 1000, 4) AS cost_per_gram
        FROM materials
        LEFT JOIN suppliers ON suppliers.id = materials.supplier_id
        ORDER BY {order_column} {sort_direction.upper()}, materials.name ASC, materials.color ASC
    """
    materials_list = db.execute(order_sql).fetchall()

    def build_sort_url(column: str, direction: str) -> str:
        params = {key: value for key, value in filters.items() if value}
        params["sort"] = column
        params["direction"] = direction
        return url_for("materials", **params)

    sort_urls = {
        key: {
            "asc": build_sort_url(key, "asc"),
            "desc": build_sort_url(key, "desc"),
        }
        for key in sort_columns
    }

    filter_options = {
        "skus": sorted({(row["sku"] or "").strip() for row in materials_list if (row["sku"] or "").strip()}),
        "material_types": sorted({(row["material_type"] or "").strip() for row in materials_list if (row["material_type"] or "").strip()}),
        "colors": sorted({(row["color"] or "").strip() for row in materials_list if (row["color"] or "").strip()}),
        "names": sorted({(row["name"] or "").strip() for row in materials_list if (row["name"] or "").strip()}),
        "manufacturers": sorted({(row["manufacturer_name"] or "").strip() for row in materials_list if (row["manufacturer_name"] or "").strip()}),
        "lots": sorted({(row["lot_number"] or "").strip() for row in materials_list if (row["lot_number"] or "").strip()}),
        "locations": sorted({(row["location"] or "").strip() for row in materials_list if (row["location"] or "").strip()}),
    }

    recent_movements = db.execute(
        """
        SELECT
            inventory_movements.*,
            materials.name AS material_name,
            materials.color AS material_color
        FROM inventory_movements
        JOIN materials ON materials.id = inventory_movements.material_id
        ORDER BY inventory_movements.created_at DESC, inventory_movements.id DESC
        LIMIT 8
        """
    ).fetchall()

    return render_template(
        "materials.html",
        materials=materials_list,
        material_types=references["material_types"],
        suppliers=references["suppliers"],
        recent_movements=recent_movements,
        next_material_number=f"{next_material_number:04d}",
        selected_material_type=selected_material_type,
        selected_supplier_id=selected_supplier_id,
        sort_key=sort_key,
        sort_direction=sort_direction,
        sort_urls=sort_urls,
        filters=filters,
        filter_options=filter_options,
        return_to=return_to,
        delete_error=request.args.get("delete_error", "").strip(),
    )


@app.route("/materials/<int:material_id>/delete", methods=["POST"])
def delete_material(material_id: int) -> str:
    db = get_db()
    blockers = [
        reference_count(db, "jobs", "material_id", material_id),
        reference_count(db, "job_materials", "material_id", material_id),
        reference_count(db, "inventory_movements", "material_id", material_id),
    ]
    if any(blockers):
        return redirect(
            url_for(
                "materials",
                delete_error="Este material está ligado a pedido ou movimentação e não foi excluído.",
            )
        )
    db.execute("UPDATE products SET material_id = NULL WHERE material_id = ?", (material_id,))
    db.execute("DELETE FROM materials WHERE id = ?", (material_id,))
    db.commit()
    return redirect(url_for("materials"))


def fetch_products(db: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            products.*,
            materials.name AS material_name,
            materials.material_type,
            materials.color AS material_color
        FROM products
        LEFT JOIN materials ON materials.id = products.material_id
        ORDER BY products.name ASC, products.id DESC
        """
    ).fetchall()
    materials_by_id = {
        row["id"]: row
        for row in db.execute(
            f"SELECT * FROM materials ORDER BY {material_order_clause()}"
        ).fetchall()
    }
    components_by_id = {
        row["id"]: row
        for row in db.execute("SELECT * FROM components ORDER BY name ASC").fetchall()
    }
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["material_lines"] = parse_product_material_lines(
            item.get("additional_material_types"),
            materials_by_id,
        )
        item["component_lines"] = parse_product_component_lines(
            item.get("accessories"),
            components_by_id,
        )
        items.append(item)
    return items


@app.route("/products", methods=["GET", "POST"])
def products() -> str:
    db = get_db()
    references = fetch_reference_data(db)
    return_to = request.values.get("return_to", "").strip()
    (
        default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour,
    ) = get_default_product_cost_rates(db)
    materials_list = db.execute(
        f"SELECT * FROM materials ORDER BY {material_order_clause()}"
    ).fetchall()
    product_error = None

    if request.method == "POST":
        try:
            product_code = generate_sequential_code(db, "products", "sku", "P")
            product_data = build_product_form_data(db)
            cursor = db.execute(
                """
                INSERT INTO products (
                    sku,
                    name,
                    category,
                    description,
                    material_id,
                    additional_material_types,
                    accessories,
                    weight_grams,
                    print_hours,
                    energy_cost_per_hour,
                    operating_cost_per_hour,
                    labor_hours,
                    labor_hourly_rate,
                    design_hours,
                    design_hourly_rate,
                    extra_cost,
                    margin_percent,
                    unit_cost,
                    sale_price,
                    stock_quantity,
                    minimum_quantity,
                    sale_channel,
                    status,
                    model_link,
                    photo_path,
                    photo_original_name,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    product_code,
                    product_data["name"],
                    product_data["category"],
                    product_data["description"],
                    product_data["material_id"],
                    product_data["additional_material_types"],
                    product_data["accessories"],
                    product_data["weight_grams"],
                    product_data["print_hours"],
                    product_data["energy_cost_per_hour"],
                    product_data["operating_cost_per_hour"],
                    product_data["labor_hours"],
                    product_data["labor_hourly_rate"],
                    product_data["design_hours"],
                    product_data["design_hourly_rate"],
                    product_data["extra_cost"],
                    product_data["margin_percent"],
                    product_data["unit_cost"],
                    product_data["sale_price"],
                    product_data["stock_quantity"],
                    product_data["minimum_quantity"],
                    product_data["sale_channel"],
                    product_data["status"],
                    product_data["model_link"],
                    None,
                    None,
                    product_data["notes"],
                ),
            )
            created_id = cursor.lastrowid
            photo_data = save_product_photo(created_id)
            if photo_data:
                db.execute(
                    """
                    UPDATE products
                    SET photo_path = ?, photo_original_name = ?
                    WHERE id = ?
                    """,
                    (
                        photo_data["photo_path"],
                        photo_data["photo_original_name"],
                        created_id,
                    ),
                )
            db.commit()
            if return_to:
                return redirect(append_query_value(return_to, "selected_product_id", created_id))
            return redirect(url_for("products"))
        except Exception as error:
            db.rollback()
            app.logger.exception("Erro ao salvar produto")
            product_error = f"Erro ao salvar produto: {error}"

    return render_template(
        "products.html",
        products=fetch_products(db),
        materials=materials_list,
        components=references["components"],
        next_product_code=generate_sequential_code(db, "products", "sku", "P"),
        return_to=return_to,
        error=product_error,
        delete_error=request.args.get("delete_error", "").strip(),
        default_product_energy_cost_per_hour=default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour=default_product_operating_cost_per_hour,
    )


@app.route("/products/<int:product_id>/edit", methods=["GET", "POST"])
def edit_product(product_id: int) -> str:
    db = get_db()
    references = fetch_reference_data(db)
    (
        default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour,
    ) = get_default_product_cost_rates(db)
    product = db.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if product is None:
        abort(404)

    materials_list = db.execute(
        f"SELECT * FROM materials ORDER BY {material_order_clause()}"
    ).fetchall()

    if request.method == "POST":
        product_data = build_product_form_data(db, existing_product=product)
        db.execute(
            """
            UPDATE products
            SET
                name = ?,
                category = ?,
                description = ?,
                material_id = ?,
                additional_material_types = ?,
                accessories = ?,
                weight_grams = ?,
                print_hours = ?,
                energy_cost_per_hour = ?,
                operating_cost_per_hour = ?,
                labor_hours = ?,
                labor_hourly_rate = ?,
                design_hours = ?,
                design_hourly_rate = ?,
                extra_cost = ?,
                margin_percent = ?,
                unit_cost = ?,
                sale_price = ?,
                stock_quantity = ?,
                minimum_quantity = ?,
                sale_channel = ?,
                status = ?,
                model_link = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                product_data["name"],
                product_data["category"],
                product_data["description"],
                product_data["material_id"],
                product_data["additional_material_types"],
                product_data["accessories"],
                product_data["weight_grams"],
                product_data["print_hours"],
                product_data["energy_cost_per_hour"],
                product_data["operating_cost_per_hour"],
                product_data["labor_hours"],
                product_data["labor_hourly_rate"],
                product_data["design_hours"],
                product_data["design_hourly_rate"],
                product_data["extra_cost"],
                product_data["margin_percent"],
                product_data["unit_cost"],
                product_data["sale_price"],
                product_data["stock_quantity"],
                product_data["minimum_quantity"],
                product_data["sale_channel"],
                product_data["status"],
                product_data["model_link"],
                product_data["notes"],
                product_id,
            ),
        )
        photo_data = save_product_photo(product_id)
        if photo_data:
            db.execute(
                """
                UPDATE products
                SET photo_path = ?, photo_original_name = ?
                WHERE id = ?
                """,
                (
                    photo_data["photo_path"],
                    photo_data["photo_original_name"],
                    product_id,
                ),
            )
        db.commit()
        return redirect(url_for("products"))

    return render_template(
        "product_edit.html",
        product=product,
        materials=materials_list,
        components=references["components"],
        product_material_lines=parse_product_material_lines(
            product["additional_material_types"],
            {row["id"]: row for row in materials_list},
        ),
        product_component_lines=parse_product_component_lines(
            product["accessories"],
            {row["id"]: row for row in references["components"]},
        ),
        default_product_energy_cost_per_hour=default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour=default_product_operating_cost_per_hour,
    )


@app.route("/products/<int:product_id>/delete", methods=["POST"])
def delete_product(product_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM products WHERE id = ?", (product_id,))
    db.commit()
    return redirect(url_for("products"))


@app.route("/inventory", methods=["GET", "POST"])
def inventory() -> str:
    db = get_db()
    if request.method == "POST":
        material_id = int(request.form["material_id"])
        quantity_grams = float(request.form["quantity_grams"])
        movement_type = request.form["movement_type"]
        notes = request.form["notes"].strip()

        material = db.execute(
            "SELECT * FROM materials WHERE id = ?",
            (material_id,),
        ).fetchone()
        delta = inventory_delta_for_type(movement_type, quantity_grams)

        if float(material["stock_grams"]) + delta < 0:
            materials_list = db.execute(
                f"SELECT * FROM materials ORDER BY {material_order_clause()}"
            ).fetchall()
            movements = db.execute(
                """
                SELECT
                    inventory_movements.*,
                    materials.name AS material_name,
                    materials.color AS material_color
                FROM inventory_movements
                JOIN materials ON materials.id = inventory_movements.material_id
                ORDER BY inventory_movements.created_at DESC, inventory_movements.id DESC
                LIMIT 20
                """
            ).fetchall()
            return render_template(
                "inventory.html",
                materials=materials_list,
                movements=movements,
                movement_types=MOVEMENT_TYPES,
                error="A movimentação deixaria o estoque negativo para esse material.",
            )

        db.execute(
            """
            INSERT INTO inventory_movements (
                material_id,
                movement_type,
                quantity_grams,
                unit_cost_per_kg,
                notes
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                material_id,
                movement_type,
                quantity_grams,
                float(material["cost_per_kg"] or 0),
                notes,
            ),
        )
        db.execute(
            "UPDATE materials SET stock_grams = stock_grams + ? WHERE id = ?",
            (delta, material_id),
        )
        db.commit()
        return redirect(url_for("inventory"))

    materials_list = db.execute(
        """
        SELECT *
        FROM materials
        ORDER BY stock_grams ASC, color COLLATE NOCASE ASC, material_type COLLATE NOCASE ASC,
            COALESCE(NULLIF(TRIM(line_series), ''), NULLIF(TRIM(name), ''), '') COLLATE NOCASE ASC,
            manufacturer_name COLLATE NOCASE ASC, sku ASC, id ASC
        """
    ).fetchall()
    movements = db.execute(
        """
        SELECT
            inventory_movements.*,
            materials.name AS material_name,
            materials.color AS material_color
        FROM inventory_movements
        JOIN materials ON materials.id = inventory_movements.material_id
        ORDER BY inventory_movements.created_at DESC, inventory_movements.id DESC
        LIMIT 20
        """
    ).fetchall()
    return render_template(
        "inventory.html",
        materials=materials_list,
        movements=movements,
        movement_types=MOVEMENT_TYPES,
        error=None,
    )


@app.route("/queries/filament-movements")
def filament_movements_query() -> str:
    db = get_db()
    materials_list = db.execute(
        """
        SELECT *
        FROM materials
        ORDER BY color COLLATE NOCASE ASC, material_type COLLATE NOCASE ASC,
            COALESCE(NULLIF(TRIM(line_series), ''), NULLIF(TRIM(name), ''), '') COLLATE NOCASE ASC,
            manufacturer_name COLLATE NOCASE ASC, sku ASC, id ASC
        """
    ).fetchall()

    selected_material_id = parse_integerish(request.args.get("material_id"))
    selected_material = None
    if materials_list:
        if not selected_material_id:
            selected_material_id = int(materials_list[0]["id"])
        selected_material = db.execute(
            """
            SELECT *
            FROM materials
            WHERE id = ?
            """,
            (selected_material_id,),
        ).fetchone()

    statement_rows: list[dict[str, Any]] = []
    opening_balance = 0.0
    current_balance = float(selected_material["stock_grams"] or 0) if selected_material else 0.0

    if selected_material is not None:
        movements = db.execute(
            """
            SELECT
                inventory_movements.*,
                jobs.id AS job_number,
                jobs.item_name AS job_item_name
            FROM inventory_movements
            LEFT JOIN jobs ON jobs.id = inventory_movements.related_job_id
            WHERE inventory_movements.material_id = ?
            ORDER BY inventory_movements.created_at ASC, inventory_movements.id ASC
            """,
            (selected_material_id,),
        ).fetchall()

        signed_total = 0.0
        for movement in movements:
            signed_total += inventory_delta_for_type(
                movement["movement_type"], float(movement["quantity_grams"] or 0)
            )
        opening_balance = current_balance - signed_total

        running_balance = opening_balance
        if opening_balance > 0:
            opening_unit_cost = float(selected_material["cost_per_kg"] or 0)
            statement_rows.append(
                {
                    "created_at": selected_material["created_at"] if "created_at" in selected_material.keys() and selected_material["created_at"] else "-",
                    "reference": "-",
                    "description": "Saldo inicial",
                    "quantity_label": f"{br_decimal(opening_balance)} g",
                    "value_label": f"R$ {br_money((opening_balance * opening_unit_cost) / 1000)}",
                    "balance_label": f"{br_decimal(running_balance)} g",
                    "direction": "Entrada",
                    "notes": "Estoque inicial do cadastro",
                }
            )

        for movement in movements:
            quantity_grams = float(movement["quantity_grams"] or 0)
            signed_quantity = inventory_delta_for_type(movement["movement_type"], quantity_grams)
            running_balance += signed_quantity
            unit_cost_per_kg = float(
                movement["unit_cost_per_kg"] or selected_material["cost_per_kg"] or 0
            )
            direction = inventory_direction_label(movement["movement_type"])
            movement_value = abs(quantity_grams) * (unit_cost_per_kg / 1000)
            related_job_number = movement["job_number"]
            if not related_job_number and (movement["notes"] or "").startswith(
                "Baixa automatica do pedido: "
            ):
                item_name = (movement["notes"] or "").replace(
                    "Baixa automatica do pedido: ", "", 1
                ).strip()
                matched_job = db.execute(
                    """
                    SELECT id
                    FROM jobs
                    WHERE item_name = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (item_name,),
                ).fetchone()
                if matched_job is not None:
                    related_job_number = matched_job["id"]
            reference = (
                f"OP/Pedido #{int(related_job_number):04d}"
                if related_job_number
                else "-"
            )
            statement_rows.append(
                {
                    "created_at": movement["created_at"],
                    "reference": reference,
                    "description": f"{direction} - {movement['movement_type']}",
                    "quantity_label": (
                        f"{'+' if signed_quantity >= 0 else '-'}{br_decimal(abs(signed_quantity))} g"
                    ),
                    "value_label": (
                        f"{'+' if signed_quantity >= 0 else '-'}R$ {br_money(movement_value)}"
                    ),
                    "balance_label": f"{br_decimal(running_balance)} g",
                    "direction": direction,
                    "notes": movement["notes"] or movement["job_item_name"] or "-",
                }
            )

    return render_template(
        "filament_movements.html",
        materials=materials_list,
        material=selected_material,
        selected_material_id=selected_material_id,
        statement_rows=statement_rows,
        opening_balance=opening_balance,
    )


@app.route("/queries")
def queries() -> str:
    db = get_db()
    totals = db.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM jobs) AS total_jobs,
            (SELECT COUNT(*) FROM jobs WHERE status NOT IN ('Orcamento', 'Cancelado', 'Entregue')) AS active_orders,
            (SELECT COUNT(*) FROM inventory_movements) AS total_movements,
            (SELECT COUNT(*) FROM materials) AS total_materials,
            (SELECT COUNT(*) FROM components) AS total_components,
            (SELECT COUNT(*) FROM products) AS total_products
        """
    ).fetchone()
    return render_template("queries.html", totals=totals)


@app.route("/queries/sales-orders")
def sales_orders_query() -> str:
    db = get_db()
    jobs = fetch_jobs(db)
    filters = {
        "created_at": request.args.get("created_at", "").strip(),
        "job_number": request.args.get("job_number", "").strip(),
        "item_name": request.args.get("item_name", "").strip(),
        "customer_name": request.args.get("customer_name", "").strip(),
        "status": request.args.get("status", "").strip(),
    }

    def normalize(value: Any) -> str:
        return str(value or "").strip().lower()

    def job_matches(job: sqlite3.Row) -> bool:
        if filters["created_at"]:
            created_at_br = br_date(job["created_at"])
            if filters["created_at"] not in {created_at_br, str(job["created_at"] or "")[:10]}:
                return False
        if filters["job_number"]:
            job_number_text = f"{int(job['id']):04d}"
            plain_job_number = str(int(job["id"]))
            if (
                filters["job_number"] not in job_number_text
                and filters["job_number"] not in plain_job_number
            ):
                return False
        if filters["item_name"] and normalize(filters["item_name"]) not in normalize(job["item_name"]):
            return False
        if filters["customer_name"] and normalize(filters["customer_name"]) not in normalize(job["customer_display"]):
            return False
        if filters["status"] and normalize(filters["status"]) not in normalize(job["status"]):
            return False
        return True

    filtered_jobs = [job for job in jobs if job_matches(job)]
    return render_template("sales_orders.html", jobs=filtered_jobs, filters=filters)


@app.route("/queries/sale-products")
def sale_products_query() -> str:
    db = get_db()
    return render_template("sale_products.html", products=fetch_products(db))


@app.route("/queries/production-orders")
def production_orders() -> str:
    db = get_db()
    return render_template("production_orders.html", jobs=fetch_jobs(db))


@app.route("/queries/production-orders/<int:job_id>", methods=["GET", "POST"])
def edit_production_order(job_id: int) -> str:
    db = get_db()
    references = fetch_reference_data(db)
    materials_list = db.execute(
        """
        SELECT
            materials.*,
            suppliers.name AS supplier_name
        FROM materials
        LEFT JOIN suppliers ON suppliers.id = materials.supplier_id
        ORDER BY materials.color COLLATE NOCASE ASC, materials.material_type COLLATE NOCASE ASC,
            COALESCE(NULLIF(TRIM(materials.line_series), ''), NULLIF(TRIM(materials.name), ''), '') COLLATE NOCASE ASC,
            materials.manufacturer_name COLLATE NOCASE ASC, materials.sku ASC, materials.id ASC
        """
    ).fetchall()
    detail = fetch_job_detail(db, job_id)

    if request.method == "POST":
        save_job_production_data(db, job_id, detail)
        return redirect(url_for("edit_production_order", job_id=job_id))

    return render_template(
        "production_order_edit.html",
        **detail,
        materials=materials_list,
        components=references["components"],
        printers=references["printers"],
        filament_dryers=references["filament_dryers"],
    )


@app.route("/materials/<int:material_id>/edit", methods=["GET", "POST"])
def edit_material(material_id: int) -> str:
    db = get_db()
    references = fetch_reference_data(db)
    material = db.execute(
        """
        SELECT *
        FROM materials
        WHERE id = ?
        """,
        (material_id,),
    ).fetchone()
    if material is None:
        abort(404)

    if request.method == "POST":
        stock_grams = float(request.form.get("stock_grams") or material["stock_grams"] or 0)
        product_cost = parse_brazilian_decimal(request.form.get("product_cost"))
        shipping_cost = parse_brazilian_decimal(request.form.get("shipping_cost"))
        store_discount = parse_brazilian_decimal(request.form.get("store_discount"))
        coupon_discount = parse_brazilian_decimal(request.form.get("coupon_discount"))
        payment_discount = parse_brazilian_decimal(request.form.get("payment_discount"))
        real_total_cost, cost_per_kg = calculate_material_costs(
            stock_grams=stock_grams,
            product_cost=product_cost,
            shipping_cost=shipping_cost,
            store_discount=store_discount,
            coupon_discount=coupon_discount,
            payment_discount=payment_discount,
        )

        fan_speed_min_percent = float(request.form.get("fan_speed_min_percent") or 0)
        fan_speed_max_percent = float(request.form.get("fan_speed_max_percent") or 0)
        flow_test_1_percent = float(request.form.get("flow_test_1_percent") or 0)
        flow_test_2_percent = float(request.form.get("flow_test_2_percent") or 0)
        db.execute(
            """
            UPDATE materials
            SET
                name = ?,
                material_type = ?,
                line_series = ?,
                color = ?,
                color_hex = ?,
                lot_number = ?,
                stock_grams = ?,
                cost_per_kg = ?,
                supplier_id = ?,
                manufacturer_name = ?,
                location = ?,
                minimum_stock_grams = ?,
                purchase_link = ?,
                product_cost = ?,
                shipping_cost = ?,
                store_discount = ?,
                coupon_discount = ?,
                payment_discount = ?,
                real_total_cost = ?,
                nozzle_temperature_c = ?,
                bed_temperature_c = ?,
                fan_speed_percent = ?,
                fan_speed_min_percent = ?,
                fan_speed_max_percent = ?,
                flow_percent = ?,
                flow_test_1_percent = ?,
                flow_test_2_percent = ?,
                retraction_distance_mm = ?,
                retraction_speed_mm_s = ?,
                pressure_advance = ?,
                print_speed_mm_s = ?,
                xy_compensation_mm = ?,
                humidity_percent = ?,
                drying_required = ?,
                notes = ?
            WHERE id = ?
            """,
            (
                request.form["line_series"].strip(),
                request.form["material_type"].strip(),
                request.form["line_series"].strip(),
                request.form["color"].strip(),
                request.form["color_hex"].strip(),
                request.form.get("lot_number", "").strip(),
                stock_grams,
                cost_per_kg,
                int(request.form["supplier_id"]) if request.form.get("supplier_id") else None,
                request.form["manufacturer_name"].strip(),
                request.form["location"].strip(),
                float(request.form["minimum_stock_grams"] or 0),
                request.form["purchase_link"].strip(),
                product_cost,
                shipping_cost,
                store_discount,
                coupon_discount,
                payment_discount,
                real_total_cost,
                float(request.form.get("nozzle_temperature_c") or 0),
                float(request.form.get("bed_temperature_c") or 0),
                fan_speed_max_percent,
                fan_speed_min_percent,
                fan_speed_max_percent,
                flow_test_1_percent,
                flow_test_1_percent,
                flow_test_2_percent,
                float(request.form.get("retraction_distance_mm") or 0),
                float(request.form.get("retraction_speed_mm_s") or 0),
                float(request.form.get("pressure_advance") or 0),
                float(request.form.get("print_speed_mm_s") or 0),
                float(request.form.get("xy_compensation_mm") or 0),
                float(request.form.get("humidity_percent") or 0),
                1 if request.form.get("drying_required") else 0,
                request.form["notes"].strip(),
                material_id,
            ),
        )
        db.commit()
        return redirect(url_for("materials"))

    return render_template(
        "material_edit.html",
        material=material,
        material_types=references["material_types"],
        suppliers=references["suppliers"],
        selected_supplier_id=parse_integerish(request.args.get("selected_supplier_id")) or material["supplier_id"],
        selected_material_type=request.args.get("selected_material_type", "").strip(),
    )


@app.route("/jobs", methods=["GET", "POST"])
def jobs() -> str:
    db = get_db()
    references = fetch_reference_data(db)
    materials_list = db.execute(
        """
        SELECT
            materials.*,
            suppliers.name AS supplier_name
        FROM materials
        LEFT JOIN suppliers ON suppliers.id = materials.supplier_id
        ORDER BY materials.color COLLATE NOCASE ASC, materials.material_type COLLATE NOCASE ASC,
            COALESCE(NULLIF(TRIM(materials.line_series), ''), NULLIF(TRIM(materials.name), ''), '') COLLATE NOCASE ASC,
            materials.manufacturer_name COLLATE NOCASE ASC, materials.sku ASC, materials.id ASC
        """
    ).fetchall()

    if request.method == "POST":
        customer_id = parse_integerish(request.form.get("customer_id"))
        item_name = request.form.get("item_name", "").strip()
        status = request.form.get("status", "").strip()
        if not customer_id or not item_name or not status:
            jobs_list = fetch_jobs(db)
            return render_template(
                "jobs.html",
                jobs=prepare_jobs_for_list(jobs_list),
                materials=materials_list,
                components=references["components"],
                products=references["products"],
                statuses=JOB_STATUSES,
                error="Preencha cliente, status e descrição do item antes de salvar o pedido.",
                customers=references["customers"],
                representatives=references["representatives"],
                partner_stores=references["partner_stores"],
                payment_terms=references["payment_terms"],
                sales_channels=references["sales_channels"],
                printers=references["printers"],
                filament_dryers=references["filament_dryers"],
                next_job_number=get_next_job_number(db),
                today_date=date.today().isoformat(),
                valid_until_date=(date.today() + timedelta(days=5)).isoformat(),
                delete_error="",
            )
        customer = db.execute(
            "SELECT * FROM customers WHERE id = ?",
            (customer_id,),
        ).fetchone()
        if customer is None:
            jobs_list = fetch_jobs(db)
            return render_template(
                "jobs.html",
                jobs=prepare_jobs_for_list(jobs_list),
                materials=materials_list,
                components=references["components"],
                products=references["products"],
                statuses=JOB_STATUSES,
                error="Selecione um cliente valido antes de salvar o pedido.",
                customers=references["customers"],
                representatives=references["representatives"],
                partner_stores=references["partner_stores"],
                payment_terms=references["payment_terms"],
                sales_channels=references["sales_channels"],
                printers=references["printers"],
                filament_dryers=references["filament_dryers"],
                next_job_number=get_next_job_number(db),
                today_date=date.today().isoformat(),
                valid_until_date=(date.today() + timedelta(days=5)).isoformat(),
                delete_error="",
            )
        try:
            material_lines = build_job_material_lines(db)
            component_lines = build_job_component_lines(db)
            service_lines = build_job_service_lines(db)
            requested_weight = sum(line["weight_grams"] for line in material_lines)
            material_stock_usage: dict[int, float] = {}
            component_stock_usage: dict[int, float] = {}

            for line in material_lines:
                material_stock_usage[line["material_id"]] = (
                    material_stock_usage.get(line["material_id"], 0.0)
                    + line["weight_grams"]
                )

            for line in component_lines:
                component_stock_usage[line["component_id"]] = (
                    component_stock_usage.get(line["component_id"], 0.0)
                    + line["quantity"]
                )

            insufficient_material = any(
                usage > float(line["material"]["stock_grams"] or 0)
                for line in material_lines
                for material_id, usage in material_stock_usage.items()
                if line["material_id"] == material_id
            )
            insufficient_component = any(
                usage > float(line["component"]["stock_quantity"] or 0)
                for line in component_lines
                for component_id, usage in component_stock_usage.items()
                if line["component_id"] == component_id
            )

            if status != "Orcamento" and (insufficient_material or insufficient_component):
                jobs_list = fetch_jobs(db)
                return render_template(
                    "jobs.html",
                    jobs=prepare_jobs_for_list(jobs_list),
                    materials=materials_list,
                    components=references["components"],
                    products=references["products"],
                    statuses=JOB_STATUSES,
                    error="Estoque insuficiente para esse pedido.",
                    customers=references["customers"],
                    representatives=references["representatives"],
                    partner_stores=references["partner_stores"],
                    payment_terms=references["payment_terms"],
                    sales_channels=references["sales_channels"],
                    printers=references["printers"],
                    filament_dryers=references["filament_dryers"],
                    next_job_number=get_next_job_number(db),
                    today_date=date.today().isoformat(),
                    valid_until_date=(date.today() + timedelta(days=5)).isoformat(),
                    delete_error="",
                )

            extra_cost = parse_brazilian_decimal(request.form.get("extra_cost"))
            margin_percent = float(request.form.get("margin_percent") or 0)
            labor_hours = float(request.form.get("labor_hours") or 0)
            labor_hourly_rate = parse_brazilian_decimal(
                request.form.get("labor_hourly_rate")
            )
            design_hours = float(request.form.get("design_hours") or 0)
            design_hourly_rate = parse_brazilian_decimal(
                request.form.get("design_hourly_rate")
            )

            customer_total = sum(line["total_price"] for line in service_lines)
            cost_summary = summarize_cost_lines(
                material_lines=material_lines,
                component_lines=component_lines,
                labor_hours=labor_hours,
                labor_hourly_rate=labor_hourly_rate,
                design_hours=design_hours,
                design_hourly_rate=design_hourly_rate,
                extra_cost=extra_cost,
                sale_total=customer_total,
            )
            total_cost = cost_summary["total_cost"]
            suggested_price = (
                customer_total
                if customer_total > 0
                else calculate_price_with_margin(total_cost, margin_percent)
            )
            print_hours = cost_summary["total_print_hours"]
            dryer_hours = cost_summary["total_dryer_hours"]
            energy_cost_per_hour = (
                round(cost_summary["energy_cost"] / print_hours, 4) if print_hours else 0.0
            )
            operating_cost_per_hour = (
                round(cost_summary["operating_cost"] / print_hours, 4) if print_hours else 0.0
            )
            dryer_cost_per_hour = (
                round(cost_summary["dryer_cost"] / dryer_hours, 4) if dryer_hours else 0.0
            )

            primary_material_id = (
                material_lines[0]["material_id"]
                if material_lines
                else int(materials_list[0]["id"])
            )

            cursor = db.execute(
                """
                INSERT INTO jobs (
                    customer_name,
                    customer_id,
                    item_name,
                    status,
                    created_at,
                    material_id,
                    weight_grams,
                    print_hours,
                    energy_cost_per_hour,
                    operating_cost_per_hour,
                    extra_cost,
                    margin_percent,
                    total_cost,
                    suggested_price,
                    notes,
                    customer_notes,
                    internal_notes,
                    representative_id,
                    partner_store_id,
                    due_date,
                    quantity,
                    sale_channel,
                    labor_hours,
                    labor_hourly_rate,
                    design_hours,
                    design_hourly_rate,
                    valid_until,
                    payment_terms,
                    model_link,
                    printer_id,
                    filament_dryer_id,
                    dryer_hours,
                    dryer_cost_per_hour,
                    customer_document_token,
                    production_document_token
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    customer["name"],
                    customer_id,
                    item_name,
                    status,
                    request.form.get("created_at", "").strip() or date.today().isoformat(),
                    primary_material_id,
                    requested_weight,
                    print_hours,
                    energy_cost_per_hour,
                    operating_cost_per_hour,
                    extra_cost,
                    margin_percent,
                    total_cost,
                    suggested_price,
                    request.form.get("internal_notes", "").strip(),
                    request.form.get("customer_notes", "").strip(),
                    request.form.get("internal_notes", "").strip(),
                    (
                        int(request.form["representative_id"])
                        if request.form.get("representative_id")
                        else None
                    ),
                    (
                        int(request.form["partner_store_id"])
                        if request.form.get("partner_store_id")
                        else None
                    ),
                    request.form.get("due_date") or None,
                    parse_integerish(request.form.get("quantity"), 1),
                    request.form.get("sale_channel", "").strip(),
                    labor_hours,
                    labor_hourly_rate,
                    design_hours,
                    design_hourly_rate,
                    request.form.get("valid_until") or None,
                    request.form.get("payment_terms", "").strip(),
                    request.form.get("model_link", "").strip(),
                    (
                        material_lines[0]["printer_id"] if material_lines else None
                    ),
                    (
                        material_lines[0]["filament_dryer_id"] if material_lines else None
                    ),
                    dryer_hours,
                    dryer_cost_per_hour,
                    make_public_document_token(),
                    make_public_document_token(),
                ),
            )
            job_id = int(cursor.lastrowid)

            for line in material_lines:
                db.execute(
                    """
                    INSERT INTO job_materials (
                        job_id,
                        material_id,
                        weight_grams,
                        print_hours,
                        printer_id,
                        energy_cost_per_hour,
                        operating_cost_per_hour,
                        filament_dryer_id,
                        dryer_hours,
                        dryer_cost_per_hour,
                        notes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        line["material_id"],
                        line["weight_grams"],
                        line["print_hours"],
                        line["printer_id"],
                        line["energy_cost_per_hour"],
                        line["operating_cost_per_hour"],
                        line["filament_dryer_id"],
                        line["dryer_hours"],
                        line["dryer_cost_per_hour"],
                        line["notes"],
                    ),
                )

            for line in component_lines:
                db.execute(
                    """
                    INSERT INTO job_components (job_id, component_id, quantity, notes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        line["component_id"],
                        line["quantity"],
                        line["notes"],
                    ),
                )

            for line in service_lines:
                db.execute(
                    """
                    INSERT INTO job_services (
                        job_id,
                        service_name,
                        category,
                        quantity,
                        hours,
                        unit_price,
                        addition_value,
                        discount_value,
                        total_price,
                        show_to_customer,
                        notes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        job_id,
                        line["service_name"],
                        line["category"],
                        line["quantity"],
                        line["hours"],
                        line["unit_price"],
                        line["addition_value"],
                        line["discount_value"],
                        line["total_price"],
                        line["notes"],
                    ),
                )

            if status != "Orcamento":
                for material_id, usage in material_stock_usage.items():
                    material_line = next(
                        (
                            line
                            for line in material_lines
                            if int(line["material_id"]) == int(material_id)
                        ),
                        None,
                    )
                    db.execute(
                        "UPDATE materials SET stock_grams = stock_grams - ? WHERE id = ?",
                        (usage, material_id),
                    )
                    db.execute(
                        """
                        INSERT INTO inventory_movements (
                            material_id,
                            movement_type,
                            quantity_grams,
                            unit_cost_per_kg,
                            related_job_id,
                            notes
                        )
                        VALUES (?, 'Consumo manual', ?, ?, ?, ?)
                        """,
                        (
                            material_id,
                            usage,
                            float(material_line["material"]["cost_per_kg"] or 0)
                            if material_line
                            else 0.0,
                            job_id,
                            f"Baixa automatica do pedido: {item_name}",
                        ),
                    )
                for component_id, usage in component_stock_usage.items():
                    db.execute(
                        """
                        UPDATE components
                        SET stock_quantity = stock_quantity - ?
                        WHERE id = ?
                        """,
                        (usage, component_id),
                    )
            db.commit()
            save_job_photos(job_id)
            db.commit()
            return redirect(url_for("jobs"))
        except Exception as error:
            db.rollback()
            app.logger.exception("Erro ao salvar pedido")
            jobs_list = fetch_jobs(db)
            return render_template(
                "jobs.html",
                jobs=prepare_jobs_for_list(jobs_list),
                materials=materials_list,
                components=references["components"],
                products=references["products"],
                statuses=JOB_STATUSES,
                error=f"Erro ao salvar pedido: {error}",
                customers=references["customers"],
                representatives=references["representatives"],
                partner_stores=references["partner_stores"],
                payment_terms=references["payment_terms"],
                sales_channels=references["sales_channels"],
                printers=references["printers"],
                filament_dryers=references["filament_dryers"],
                next_job_number=get_next_job_number(db),
                today_date=date.today().isoformat(),
                valid_until_date=(date.today() + timedelta(days=5)).isoformat(),
                delete_error="",
            )

    jobs_list = fetch_jobs(db)
    return render_template(
        "jobs.html",
        jobs=prepare_jobs_for_list(jobs_list),
        materials=materials_list,
        components=references["components"],
        products=references["products"],
        statuses=JOB_STATUSES,
        error=None,
        customers=references["customers"],
        representatives=references["representatives"],
        partner_stores=references["partner_stores"],
        payment_terms=references["payment_terms"],
        sales_channels=references["sales_channels"],
        printers=references["printers"],
        filament_dryers=references["filament_dryers"],
        next_job_number=get_next_job_number(db),
        today_date=date.today().isoformat(),
        valid_until_date=(date.today() + timedelta(days=5)).isoformat(),
        delete_error=request.args.get("delete_error", "").strip(),
    )


@app.route("/jobs/<int:job_id>/delete", methods=["POST"])
def delete_job(job_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM job_materials WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM job_components WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM job_services WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM job_photos WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    db.commit()
    return redirect(url_for("jobs"))


def fetch_jobs(db: sqlite3.Connection) -> list[sqlite3.Row]:
    return db.execute(
        """
        SELECT
            jobs.*,
            materials.name AS material_name,
            materials.color AS material_color,
            printers.name AS printer_name,
            printers.model AS printer_model,
            printers.technology AS printer_technology,
            filament_dryers.brand AS dryer_brand,
            filament_dryers.model AS dryer_model,
            filament_dryers.dryer_type AS dryer_type,
            representatives.name AS representative_name,
            partner_stores.name AS partner_store_name,
            (SELECT COUNT(*) FROM job_materials WHERE job_materials.job_id = jobs.id) AS material_lines_count,
            (SELECT COUNT(*) FROM job_components WHERE job_components.job_id = jobs.id) AS component_lines_count,
            (SELECT COUNT(*) FROM job_services WHERE job_services.job_id = jobs.id) AS service_lines_count,
            COALESCE(customers.name, jobs.customer_name) AS customer_display,
            customers.phone AS customer_phone
        FROM jobs
        JOIN materials ON materials.id = jobs.material_id
        LEFT JOIN printers ON printers.id = jobs.printer_id
        LEFT JOIN filament_dryers ON filament_dryers.id = jobs.filament_dryer_id
        LEFT JOIN customers ON customers.id = jobs.customer_id
        LEFT JOIN representatives ON representatives.id = jobs.representative_id
        LEFT JOIN partner_stores ON partner_stores.id = jobs.partner_store_id
        ORDER BY jobs.created_at DESC, jobs.id DESC
        """
    ).fetchall()


def prepare_jobs_for_list(jobs: list[sqlite3.Row]) -> list[dict[str, Any]]:
    prepared_jobs: list[dict[str, Any]] = []
    for job in jobs:
        item = dict(job)
        customer_url = url_for(
            "public_job_customer_document",
            token=item["customer_document_token"],
            _external=True,
        )
        production_url = url_for(
            "public_job_production_document",
            token=item["production_document_token"],
            _external=True,
        )
        customer_message = (
            f"Olá, segue o pedido #{int(item['id']):04d}: {customer_url}"
        )
        production_message = (
            f"Olá, segue a ordem de produção do pedido #{int(item['id']):04d}: {production_url}"
        )
        item["whatsapp_customer_url"] = build_whatsapp_link(
            item.get("customer_phone"), customer_message
        )
        item["whatsapp_production_url"] = build_whatsapp_link(
            item.get("customer_phone"), production_message
        )
        prepared_jobs.append(item)
    return prepared_jobs


def save_job_production_data(
    db: sqlite3.Connection,
    job_id: int,
    detail: dict[str, Any],
) -> None:
    material_lines = build_job_material_lines(db)
    component_lines = build_job_component_lines(db)
    requested_weight = sum(line["weight_grams"] for line in material_lines)
    extra_cost = parse_brazilian_decimal(request.form.get("extra_cost"))
    margin_percent = float(request.form.get("margin_percent") or 0)
    labor_hours = float(request.form.get("labor_hours") or 0)
    labor_hourly_rate = parse_brazilian_decimal(request.form.get("labor_hourly_rate"))
    design_hours = float(request.form.get("design_hours") or 0)
    design_hourly_rate = parse_brazilian_decimal(request.form.get("design_hourly_rate"))
    sale_total = sum(float(line["total_price"] or 0) for line in detail["service_lines"])
    cost_summary = summarize_cost_lines(
        material_lines=material_lines,
        component_lines=component_lines,
        labor_hours=labor_hours,
        labor_hourly_rate=labor_hourly_rate,
        design_hours=design_hours,
        design_hourly_rate=design_hourly_rate,
        extra_cost=extra_cost,
        sale_total=sale_total,
    )
    total_cost = cost_summary["total_cost"]
    print_hours = cost_summary["total_print_hours"]
    dryer_hours = cost_summary["total_dryer_hours"]
    energy_cost_per_hour = (
        round(cost_summary["energy_cost"] / print_hours, 4) if print_hours else 0.0
    )
    operating_cost_per_hour = (
        round(cost_summary["operating_cost"] / print_hours, 4) if print_hours else 0.0
    )
    dryer_cost_per_hour = (
        round(cost_summary["dryer_cost"] / dryer_hours, 4) if dryer_hours else 0.0
    )
    primary_material_id = (
        material_lines[0]["material_id"]
        if material_lines
        else int(detail["job"]["material_id"])
    )

    db.execute(
        """
        UPDATE jobs
        SET
            material_id = ?,
            weight_grams = ?,
            print_hours = ?,
            energy_cost_per_hour = ?,
            operating_cost_per_hour = ?,
            extra_cost = ?,
            margin_percent = ?,
            total_cost = ?,
            internal_notes = ?,
            notes = ?,
            printer_id = ?,
            filament_dryer_id = ?,
            dryer_hours = ?,
            dryer_cost_per_hour = ?,
            labor_hours = ?,
            labor_hourly_rate = ?,
            design_hours = ?,
            design_hourly_rate = ?
        WHERE id = ?
        """,
        (
            primary_material_id,
            requested_weight,
            print_hours,
            energy_cost_per_hour,
            operating_cost_per_hour,
            extra_cost,
            margin_percent,
            total_cost,
            request.form.get("internal_notes", "").strip(),
            request.form.get("internal_notes", "").strip(),
            (material_lines[0]["printer_id"] if material_lines else None),
            (material_lines[0]["filament_dryer_id"] if material_lines else None),
            dryer_hours,
            dryer_cost_per_hour,
            labor_hours,
            labor_hourly_rate,
            design_hours,
            design_hourly_rate,
            job_id,
        ),
    )
    db.execute("DELETE FROM job_materials WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM job_components WHERE job_id = ?", (job_id,))

    for line in material_lines:
        db.execute(
            """
            INSERT INTO job_materials (
                job_id,
                material_id,
                weight_grams,
                print_hours,
                printer_id,
                energy_cost_per_hour,
                operating_cost_per_hour,
                filament_dryer_id,
                dryer_hours,
                dryer_cost_per_hour,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                line["material_id"],
                line["weight_grams"],
                line["print_hours"],
                line["printer_id"],
                line["energy_cost_per_hour"],
                line["operating_cost_per_hour"],
                line["filament_dryer_id"],
                line["dryer_hours"],
                line["dryer_cost_per_hour"],
                line["notes"],
            ),
        )
    for line in component_lines:
        db.execute(
            """
            INSERT INTO job_components (job_id, component_id, quantity, notes)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, line["component_id"], line["quantity"], line["notes"]),
        )

    db.commit()


def save_job_commercial_data(
    db: sqlite3.Connection,
    job_id: int,
    detail: dict[str, Any],
) -> None:
    customer_id = parse_integerish(request.form.get("customer_id"))
    item_name = request.form.get("item_name", "").strip()
    status = request.form.get("status", "").strip()
    if not customer_id or not item_name or not status:
        raise ValueError("Campos comerciais obrigatorios ausentes.")
    customer = db.execute(
        "SELECT * FROM customers WHERE id = ?",
        (customer_id,),
    ).fetchone()
    if customer is None:
        raise ValueError("Cliente invalido para atualizacao comercial.")
    service_lines = build_job_service_lines(db)
    customer_total = sum(line["total_price"] for line in service_lines)
    suggested_price = (
        round(customer_total, 2)
        if customer_total > 0
        else float(detail["job"]["suggested_price"] or 0)
    )

    db.execute(
        """
        UPDATE jobs
        SET
            customer_name = ?,
            customer_id = ?,
            item_name = ?,
            status = ?,
            created_at = ?,
            representative_id = ?,
            partner_store_id = ?,
            due_date = ?,
            quantity = ?,
            sale_channel = ?,
            suggested_price = ?,
            customer_notes = ?,
            valid_until = ?,
            payment_terms = ?,
            model_link = ?
        WHERE id = ?
        """,
        (
            customer["name"],
            customer_id,
            item_name,
            status,
            request.form.get("created_at", "").strip()
            or str(detail["job"]["created_at"])[:10],
            (
                int(request.form["representative_id"])
                if request.form.get("representative_id")
                else None
            ),
            (
                int(request.form["partner_store_id"])
                if request.form.get("partner_store_id")
                else None
            ),
            request.form.get("due_date") or None,
            parse_integerish(request.form.get("quantity"), 1),
            request.form.get("sale_channel", "").strip(),
            suggested_price,
            request.form.get("customer_notes", "").strip(),
            request.form.get("valid_until") or None,
            request.form.get("payment_terms", "").strip(),
            request.form.get("model_link", "").strip(),
            job_id,
        ),
    )
    db.execute("DELETE FROM job_services WHERE job_id = ?", (job_id,))
    for line in service_lines:
        db.execute(
            """
            INSERT INTO job_services (
                job_id,
                service_name,
                category,
                quantity,
                hours,
                unit_price,
                addition_value,
                discount_value,
                total_price,
                show_to_customer,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                job_id,
                line["service_name"],
                line["category"],
                line["quantity"],
                line["hours"],
                line["unit_price"],
                line["addition_value"],
                line["discount_value"],
                line["total_price"],
                line["notes"],
            ),
        )
    db.commit()


@app.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
def edit_job(job_id: int) -> str:
    db = get_db()
    references = fetch_reference_data(db)
    materials_list = db.execute(
        """
        SELECT
            materials.*,
            suppliers.name AS supplier_name
        FROM materials
        LEFT JOIN suppliers ON suppliers.id = materials.supplier_id
        ORDER BY materials.color COLLATE NOCASE ASC, materials.material_type COLLATE NOCASE ASC,
            COALESCE(NULLIF(TRIM(materials.line_series), ''), NULLIF(TRIM(materials.name), ''), '') COLLATE NOCASE ASC,
            materials.manufacturer_name COLLATE NOCASE ASC, materials.sku ASC, materials.id ASC
        """
    ).fetchall()
    detail = fetch_job_detail(db, job_id)

    if request.method == "POST":
        if "material_id" not in request.form and "component_id" not in request.form:
            try:
                save_job_commercial_data(db, job_id, detail)
            except ValueError as error:
                return render_template(
                    "job_edit.html",
                    **detail,
                    materials=materials_list,
                    components=references["components"],
                    products=references["products"],
                    statuses=JOB_STATUSES,
                    customers=references["customers"],
                    representatives=references["representatives"],
                    partner_stores=references["partner_stores"],
                    payment_terms=references["payment_terms"],
                    sales_channels=references["sales_channels"],
                    printers=references["printers"],
                    filament_dryers=references["filament_dryers"],
                    today_date=date.today().isoformat(),
                    error=str(error),
                )
            return redirect(url_for("edit_job", job_id=job_id))

        customer_id = parse_integerish(request.form.get("customer_id"))
        item_name = request.form.get("item_name", "").strip()
        status = request.form.get("status", "").strip()
        if not customer_id or not item_name or not status:
            return render_template(
                "job_edit.html",
                **detail,
                materials=materials_list,
                components=references["components"],
                products=references["products"],
                statuses=JOB_STATUSES,
                customers=references["customers"],
                representatives=references["representatives"],
                partner_stores=references["partner_stores"],
                payment_terms=references["payment_terms"],
                sales_channels=references["sales_channels"],
                printers=references["printers"],
                filament_dryers=references["filament_dryers"],
                today_date=date.today().isoformat(),
                error="Preencha cliente, status e descrição do item antes de salvar o pedido.",
            )

        customer = db.execute(
            "SELECT * FROM customers WHERE id = ?",
            (customer_id,),
        ).fetchone()
        if customer is None:
            return render_template(
                "job_edit.html",
                **detail,
                materials=materials_list,
                components=references["components"],
                products=references["products"],
                statuses=JOB_STATUSES,
                customers=references["customers"],
                representatives=references["representatives"],
                partner_stores=references["partner_stores"],
                payment_terms=references["payment_terms"],
                sales_channels=references["sales_channels"],
                printers=references["printers"],
                filament_dryers=references["filament_dryers"],
                today_date=date.today().isoformat(),
                error="Selecione um cliente valido antes de salvar o pedido.",
            )
        material_lines = build_job_material_lines(db)
        component_lines = build_job_component_lines(db)
        service_lines = build_job_service_lines(db)
        requested_weight = sum(line["weight_grams"] for line in material_lines)
        extra_cost = parse_brazilian_decimal(request.form.get("extra_cost"))
        margin_percent = float(request.form.get("margin_percent") or 0)
        labor_hours = float(request.form.get("labor_hours") or 0)
        labor_hourly_rate = parse_brazilian_decimal(
            request.form.get("labor_hourly_rate")
        )
        design_hours = float(request.form.get("design_hours") or 0)
        design_hourly_rate = parse_brazilian_decimal(
            request.form.get("design_hourly_rate")
        )
        customer_total = sum(line["total_price"] for line in service_lines)
        cost_summary = summarize_cost_lines(
            material_lines=material_lines,
            component_lines=component_lines,
            labor_hours=labor_hours,
            labor_hourly_rate=labor_hourly_rate,
            design_hours=design_hours,
            design_hourly_rate=design_hourly_rate,
            extra_cost=extra_cost,
            sale_total=customer_total,
        )
        total_cost = cost_summary["total_cost"]
        suggested_price = (
            customer_total
            if customer_total > 0
            else calculate_price_with_margin(total_cost, margin_percent)
        )
        print_hours = cost_summary["total_print_hours"]
        dryer_hours = cost_summary["total_dryer_hours"]
        energy_cost_per_hour = (
            round(cost_summary["energy_cost"] / print_hours, 4) if print_hours else 0.0
        )
        operating_cost_per_hour = (
            round(cost_summary["operating_cost"] / print_hours, 4) if print_hours else 0.0
        )
        dryer_cost_per_hour = (
            round(cost_summary["dryer_cost"] / dryer_hours, 4) if dryer_hours else 0.0
        )
        primary_material_id = (
            material_lines[0]["material_id"]
            if material_lines
            else int(detail["job"]["material_id"])
        )

        db.execute(
            """
            UPDATE jobs
            SET
                customer_name = ?,
                customer_id = ?,
                item_name = ?,
                status = ?,
                created_at = ?,
                material_id = ?,
                weight_grams = ?,
                print_hours = ?,
                energy_cost_per_hour = ?,
                operating_cost_per_hour = ?,
                extra_cost = ?,
                margin_percent = ?,
                total_cost = ?,
                suggested_price = ?,
                notes = ?,
                customer_notes = ?,
                internal_notes = ?,
                representative_id = ?,
                partner_store_id = ?,
                due_date = ?,
                quantity = ?,
                sale_channel = ?,
                printer_id = ?,
                filament_dryer_id = ?,
                dryer_hours = ?,
                dryer_cost_per_hour = ?,
                labor_hours = ?,
                labor_hourly_rate = ?,
                design_hours = ?,
                design_hourly_rate = ?,
                valid_until = ?,
                payment_terms = ?,
                model_link = ?
            WHERE id = ?
            """,
            (
                customer["name"],
                customer_id,
                item_name,
                status,
                request.form.get("created_at", "").strip()
                or str(detail["job"]["created_at"])[:10],
                primary_material_id,
                requested_weight,
                print_hours,
                energy_cost_per_hour,
                operating_cost_per_hour,
                extra_cost,
                margin_percent,
                total_cost,
                suggested_price,
                request.form.get("internal_notes", "").strip(),
                request.form.get("customer_notes", "").strip(),
                request.form.get("internal_notes", "").strip(),
                (
                    int(request.form["representative_id"])
                    if request.form.get("representative_id")
                    else None
                ),
                (
                    int(request.form["partner_store_id"])
                    if request.form.get("partner_store_id")
                    else None
                ),
                request.form.get("due_date") or None,
                parse_integerish(request.form.get("quantity"), 1),
                request.form.get("sale_channel", "").strip(),
                (
                    material_lines[0]["printer_id"] if material_lines else None
                ),
                (
                    material_lines[0]["filament_dryer_id"] if material_lines else None
                ),
                dryer_hours,
                dryer_cost_per_hour,
                labor_hours,
                labor_hourly_rate,
                design_hours,
                design_hourly_rate,
                request.form.get("valid_until") or None,
                request.form.get("payment_terms", "").strip(),
                request.form.get("model_link", "").strip(),
                job_id,
            ),
        )
        db.execute("DELETE FROM job_materials WHERE job_id = ?", (job_id,))
        db.execute("DELETE FROM job_components WHERE job_id = ?", (job_id,))
        db.execute("DELETE FROM job_services WHERE job_id = ?", (job_id,))

        for line in material_lines:
            db.execute(
                """
                INSERT INTO job_materials (
                    job_id,
                    material_id,
                    weight_grams,
                    print_hours,
                    printer_id,
                    energy_cost_per_hour,
                    operating_cost_per_hour,
                    filament_dryer_id,
                    dryer_hours,
                    dryer_cost_per_hour,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    line["material_id"],
                    line["weight_grams"],
                    line["print_hours"],
                    line["printer_id"],
                    line["energy_cost_per_hour"],
                    line["operating_cost_per_hour"],
                    line["filament_dryer_id"],
                    line["dryer_hours"],
                    line["dryer_cost_per_hour"],
                    line["notes"],
                ),
            )
        for line in component_lines:
            db.execute(
                """
                INSERT INTO job_components (job_id, component_id, quantity, notes)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, line["component_id"], line["quantity"], line["notes"]),
            )
        for line in service_lines:
            db.execute(
                """
                INSERT INTO job_services (
                    job_id,
                    service_name,
                    category,
                    quantity,
                    hours,
                    unit_price,
                    addition_value,
                    discount_value,
                    total_price,
                    show_to_customer,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    job_id,
                    line["service_name"],
                    line["category"],
                    line["quantity"],
                    line["hours"],
                    line["unit_price"],
                    line["addition_value"],
                    line["discount_value"],
                    line["total_price"],
                    line["notes"],
                ),
            )
        save_job_photos(job_id)
        db.commit()
        return redirect(url_for("jobs"))

    return render_template(
        "job_edit.html",
        **detail,
        materials=materials_list,
        components=references["components"],
        products=references["products"],
        statuses=JOB_STATUSES,
        customers=references["customers"],
        representatives=references["representatives"],
        partner_stores=references["partner_stores"],
        payment_terms=references["payment_terms"],
        sales_channels=references["sales_channels"],
        printers=references["printers"],
        filament_dryers=references["filament_dryers"],
        today_date=date.today().isoformat(),
    )


def fetch_job_detail(db: sqlite3.Connection, job_id: int) -> dict[str, Any]:
    job = db.execute(
        """
        SELECT
            jobs.*,
            customers.document AS customer_document,
            customers.phone AS customer_phone,
            customers.email AS customer_email,
            customers.city AS customer_city,
            printers.name AS printer_name,
            printers.model AS printer_model,
            printers.technology AS printer_technology,
            printers.hourly_cost AS printer_hourly_cost,
            filament_dryers.brand AS dryer_brand,
            filament_dryers.model AS dryer_model,
            filament_dryers.dryer_type AS dryer_type,
            representatives.name AS representative_name,
            partner_stores.name AS partner_store_name,
            COALESCE(customers.name, jobs.customer_name) AS customer_display
        FROM jobs
        LEFT JOIN customers ON customers.id = jobs.customer_id
        LEFT JOIN printers ON printers.id = jobs.printer_id
        LEFT JOIN filament_dryers ON filament_dryers.id = jobs.filament_dryer_id
        LEFT JOIN representatives ON representatives.id = jobs.representative_id
        LEFT JOIN partner_stores ON partner_stores.id = jobs.partner_store_id
        WHERE jobs.id = ?
        """,
        (job_id,),
    ).fetchone()
    if job is None:
        abort(404)

    material_lines = db.execute(
        """
        SELECT
            job_materials.*,
            materials.name AS material_name,
            materials.material_type,
            materials.color,
            materials.color_hex,
            materials.manufacturer_name,
            materials.cost_per_kg,
            materials.stock_grams,
            materials.location,
            printers.name AS printer_name,
            printers.model AS printer_model,
            printers.energy_watts AS printer_energy_watts,
            printers.kwh_cost AS printer_kwh_cost,
            printers.hourly_cost AS printer_hourly_cost,
            filament_dryers.brand AS dryer_brand,
            filament_dryers.model AS dryer_model
        FROM job_materials
        JOIN materials ON materials.id = job_materials.material_id
        LEFT JOIN printers ON printers.id = job_materials.printer_id
        LEFT JOIN filament_dryers ON filament_dryers.id = job_materials.filament_dryer_id
        WHERE job_materials.job_id = ?
        ORDER BY job_materials.id ASC
        """,
        (job_id,),
    ).fetchall()
    component_lines = db.execute(
        """
        SELECT
            job_components.*,
            components.name AS component_name,
            components.component_type,
            components.sku,
            components.unit_measure,
            components.unit_cost,
            components.stock_quantity,
            components.location
        FROM job_components
        JOIN components ON components.id = job_components.component_id
        WHERE job_components.job_id = ?
        ORDER BY job_components.id ASC
        """,
        (job_id,),
    ).fetchall()
    service_lines = db.execute(
        """
        SELECT *
        FROM job_services
        WHERE job_id = ?
        ORDER BY id ASC
        """,
        (job_id,),
    ).fetchall()
    photo_lines = db.execute(
        """
        SELECT *
        FROM job_photos
        WHERE job_id = ?
        ORDER BY id ASC
        """,
        (job_id,),
    ).fetchall()
    normalized_material_lines = []
    for index, line in enumerate(material_lines):
        printer_energy_rate = float(line["energy_cost_per_hour"] or 0)
        printer_operating_rate = float(line["operating_cost_per_hour"] or 0)
        if not line["printer_name"] and len(material_lines) == 1 and job["printer_name"]:
            line = {
                **dict(line),
                "printer_name": job["printer_name"],
                "printer_model": job["printer_model"],
                "printer_energy_watts": 0,
                "printer_kwh_cost": 0,
                "printer_hourly_cost": job["printer_hourly_cost"],
            }
        if not printer_energy_rate and not printer_operating_rate and line["printer_name"]:
            printer_energy_rate = (
                (float(line["printer_energy_watts"] or 0) / 1000)
                * float(line["printer_kwh_cost"] or 0)
            )
            printer_operating_rate = max(
                float(line["printer_hourly_cost"] or 0) - printer_energy_rate,
                0,
            )
        if len(material_lines) == 1:
            if printer_energy_rate <= 0:
                printer_energy_rate = float(job["energy_cost_per_hour"] or 0)
            if printer_operating_rate <= 0:
                printer_operating_rate = float(job["operating_cost_per_hour"] or 0)

        line_print_hours = float(line["print_hours"] or 0)
        if line_print_hours <= 0 and len(material_lines) == 1:
            line_print_hours = float(job["print_hours"] or 0)

        line_dryer_hours = float(line["dryer_hours"] or 0)
        if not line["dryer_brand"] and len(material_lines) == 1 and job["dryer_brand"]:
            line = {
                **dict(line),
                "dryer_brand": job["dryer_brand"],
                "dryer_model": job["dryer_model"],
            }
        if line_dryer_hours <= 0 and line["dryer_brand"] and len(material_lines) == 1:
            line_dryer_hours = float(job["dryer_hours"] or 0) or line_print_hours

        line_dryer_rate = float(line["dryer_cost_per_hour"] or 0)
        if line_dryer_rate <= 0 and line["dryer_brand"]:
            line_dryer_rate = float(job["dryer_cost_per_hour"] or 0)

        normalized_material_lines.append(
            {
                **dict(line),
                "print_hours": line_print_hours,
                "energy_cost_per_hour": printer_energy_rate,
                "operating_cost_per_hour": printer_operating_rate,
                "dryer_hours": line_dryer_hours,
                "dryer_cost_per_hour": line_dryer_rate,
                "printer_label": (
                    f"{line['printer_name']} - {line['printer_model']}"
                    if line["printer_name"] and line["printer_model"]
                    else (line["printer_name"] or "Sem impressora")
                ),
                "dryer_label": (
                    f"{line['dryer_brand']} {line['dryer_model']}".strip()
                    if line["dryer_brand"]
                    else ""
                ),
            }
        )

    normalized_component_lines = [dict(line) for line in component_lines]
    cost_summary = summarize_cost_lines(
        material_lines=normalized_material_lines,
        component_lines=normalized_component_lines,
        labor_hours=float(job["labor_hours"] or 0),
        labor_hourly_rate=float(job["labor_hourly_rate"] or 0),
        design_hours=float(job["design_hours"] or 0),
        design_hourly_rate=float(job["design_hourly_rate"] or 0),
        extra_cost=float(job["extra_cost"] or 0),
        sale_total=float(job["suggested_price"] or 0),
    )
    if (
        cost_summary["energy_cost"] <= 0
        and float(job["print_hours"] or 0) > 0
        and (job["printer_name"] or job["energy_cost_per_hour"])
    ):
        fallback_energy = round(
            float(job["print_hours"] or 0) * float(job["energy_cost_per_hour"] or 0), 2
        )
        fallback_operating = round(
            float(job["print_hours"] or 0)
            * float(job["operating_cost_per_hour"] or 0),
            2,
        )
        printer_label = (
            f"{job['printer_name']} - {job['printer_model']}"
            if job["printer_name"] and job["printer_model"]
            else (job["printer_name"] or "Sem impressora")
        )
        cost_summary["energy_cost"] = fallback_energy
        cost_summary["operating_cost"] = fallback_operating
        cost_summary["total_print_hours"] = round(float(job["print_hours"] or 0), 2)
        cost_summary["breakdowns"]["energy"] = [
            {
                "label": printer_label,
                "base": f"{br_decimal(job['print_hours'])} h",
                "rate": f"R$ {br_money(job['energy_cost_per_hour'])}/h",
                "total": fallback_energy,
            }
        ]
        cost_summary["breakdowns"]["operating"] = [
            {
                "label": printer_label,
                "base": f"{br_decimal(job['print_hours'])} h",
                "rate": f"R$ {br_money(job['operating_cost_per_hour'])}/h",
                "total": fallback_operating,
            }
        ]
    if (
        cost_summary["dryer_cost"] <= 0
        and (job["dryer_brand"] or job["dryer_cost_per_hour"])
        and (
            float(job["dryer_hours"] or 0) > 0
            or float(job["print_hours"] or 0) > 0
        )
    ):
        fallback_dryer_hours = float(job["dryer_hours"] or 0) or float(
            job["print_hours"] or 0
        )
        dryer_label = (
            f"{job['dryer_brand']} {job['dryer_model']}".strip()
            if job["dryer_brand"]
            else "Sem secador"
        )
        fallback_dryer = round(
            fallback_dryer_hours * float(job["dryer_cost_per_hour"] or 0), 2
        )
        cost_summary["dryer_cost"] = fallback_dryer
        cost_summary["total_dryer_hours"] = round(fallback_dryer_hours, 2)
        cost_summary["breakdowns"]["dryers"] = [
            {
                "label": dryer_label,
                "base": f"{br_decimal(fallback_dryer_hours)} h",
                "rate": f"R$ {br_money(job['dryer_cost_per_hour'])}/h",
                "total": fallback_dryer,
            }
        ]
    cost_summary["total_cost"] = round(
        cost_summary["material_cost"]
        + cost_summary["component_cost"]
        + cost_summary["energy_cost"]
        + cost_summary["operating_cost"]
        + cost_summary["dryer_cost"]
        + cost_summary["labor_cost"]
        + cost_summary["design_cost"]
        + cost_summary["extra_cost"],
        2,
    )
    cost_summary["suggested_price"] = float(job["suggested_price"] or 0)
    cost_summary["profit"] = round(
        cost_summary["suggested_price"] - cost_summary["total_cost"], 2
    )
    return {
        "job": job,
        "material_lines": normalized_material_lines,
        "component_lines": normalized_component_lines,
        "service_lines": service_lines,
        "photo_lines": photo_lines,
        "cost_summary": cost_summary,
    }


def fetch_job_detail_by_token(
    db: sqlite3.Connection,
    token: str,
    token_column: str,
) -> dict[str, Any]:
    if token_column not in {"customer_document_token", "production_document_token"}:
        abort(404)
    token = str(token or "").strip()
    if len(token) < 24:
        abort(404)
    row = db.execute(
        f"SELECT id FROM jobs WHERE {token_column} = ?",
        (token,),
    ).fetchone()
    if row is None:
        abort(404)
    return fetch_job_detail(db, int(row["id"]))


@app.route("/jobs/<int:job_id>/cliente")
def job_customer_document(job_id: int) -> str:
    detail = fetch_job_detail(get_db(), job_id)
    return render_template("job_customer_document.html", **detail, public_view=False)


@app.route("/jobs/<int:job_id>/producao")
def job_production_document(job_id: int) -> str:
    detail = fetch_job_detail(get_db(), job_id)
    return render_template("job_production_document.html", **detail, public_view=False)


@app.route("/publico/pedido/<token>")
def public_job_customer_document(token: str) -> str:
    detail = fetch_job_detail_by_token(get_db(), token, "customer_document_token")
    return render_template("job_customer_document.html", **detail, public_view=True)


@app.route("/publico/ordem/<token>")
def public_job_production_document(token: str) -> str:
    detail = fetch_job_detail_by_token(get_db(), token, "production_document_token")
    return render_template("job_production_document.html", **detail, public_view=True)


@app.route("/pricing", methods=["GET", "POST"])
def pricing() -> str:
    db = get_db()
    (
        default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour,
    ) = get_default_product_cost_rates(db)
    materials_list = db.execute(
        f"SELECT * FROM materials ORDER BY {material_order_clause()}"
    ).fetchall()
    result = None

    if request.method == "POST":
        material = db.execute(
            "SELECT * FROM materials WHERE id = ?",
            (int(request.form["material_id"]),),
        ).fetchone()

        total_cost, suggested_price = calculate_job_values(
            material_cost_per_kg=float(material["cost_per_kg"]),
            weight_grams=float(request.form["weight_grams"]),
            print_hours=float(request.form["print_hours"]),
            energy_cost_per_hour=parse_brazilian_decimal(
                request.form.get("energy_cost_per_hour")
            ),
            operating_cost_per_hour=parse_brazilian_decimal(
                request.form.get("operating_cost_per_hour")
            ),
            extra_cost=parse_brazilian_decimal(request.form.get("extra_cost")),
            margin_percent=parse_loose_float(request.form.get("margin_percent"), 0.0),
        )
        result = {
            "material_name": material["name"],
            "material_color": material["color"],
            "weight_grams": float(request.form["weight_grams"]),
            "print_hours": float(request.form["print_hours"]),
            "extra_cost": parse_brazilian_decimal(request.form.get("extra_cost")),
            "margin_percent": parse_loose_float(request.form.get("margin_percent"), 0.0),
            "total_cost": total_cost,
            "suggested_price": suggested_price,
            "cost_per_gram": round(float(material["cost_per_kg"]) / 1000, 4),
        }

    return render_template(
        "pricing.html",
        materials=materials_list,
        result=result,
        default_product_energy_cost_per_hour=default_product_energy_cost_per_hour,
        default_product_operating_cost_per_hour=default_product_operating_cost_per_hour,
    )


with app.app_context():
    init_db()


if __name__ == "__main__":
    app.run(debug=True)
