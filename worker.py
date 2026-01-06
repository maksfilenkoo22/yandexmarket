import time
import sqlite3
import requests
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

def main_loop():
    logging.info("Worker daemon started")
    while True:
        try:
            main()
        except Exception as e:
            logging.exception(f"Fatal error in worker loop: {e}")
        time.sleep(3600)  # проверка заказов раз в минуту

from config import (
    API_TOKEN,
    BUSINESS_ID,
    CAMPAIGN_ID,
    DB_PATH,
    LOG_PATH
)

BASE_URL = "https://api.partner.market.yandex.ru"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# --------------------
# LOGGING
# --------------------
handler = RotatingFileHandler(
    LOG_PATH,
    maxBytes=10 * 1024 * 1024,  # 10 MB
    backupCount=3
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
    format="%(asctime)s %(levelname)s %(message)s"
)

# --------------------
# BUSINESS API
# --------------------
def get_orders():
    url = f"{BASE_URL}/v1/businesses/{BUSINESS_ID}/orders"
    params = {"limit": 20}

    r = requests.post(
        url,
        headers=HEADERS,
        params=params,
        json={},  # обязательное пустое тело
        timeout=30
    )
    r.raise_for_status()
    return r.json().get("orders", [])


# --------------------
# ORDERS STATUS (DB)
# --------------------
def upsert_order_status(order):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    order_id = order.get("orderId")   # PRIMARY KEY
    item_id = order.get("items", [{}])[0].get("id")  # <-- ВАЖНО
    status = order.get("status")
    payment = order.get("paymentType")
    delivery = order.get("delivery", {}).get("type")
    now = datetime.now(timezone.utc).isoformat()

    cur.execute(
        "SELECT status FROM orders_status WHERE order_id = ?",
        (order_id,)
    )
    row = cur.fetchone()

    if row is None:
        # новый заказ
        cur.execute("""
            INSERT INTO orders_status (
                order_id,
                id,
                status,
                payment_type,
                delivery_type,
                first_seen_at,
                last_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            order_id,
            item_id,
            status,
            payment,
            delivery,
            now,
            now
        ))

    elif row[0] != status:
        # статус изменился
        cur.execute("""
            UPDATE orders_status
            SET
                id = ?,
                status = ?,
                payment_type = ?,
                delivery_type = ?,
                last_updated_at = ?
            WHERE order_id = ?
        """, (
            item_id,
            status,
            payment,
            delivery,
            now,
            order_id
        ))

    conn.commit()
    conn.close()



# --------------------
# DIGITAL ACCOUNTS
# --------------------
def order_already_processed(order_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM digital_accounts WHERE order_id = ? LIMIT 1",
        (order_id,)
    )

    exists = cur.fetchone() is not None
    conn.close()
    return exists


def reserve_accounts(order_id, count):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute("BEGIN IMMEDIATE")

        cur.execute("""
            SELECT *
            FROM digital_accounts
            WHERE status = 'free'
            LIMIT ?
        """, (count,))
        accounts = cur.fetchall()

        if len(accounts) < count:
            conn.rollback()
            return None

        ids = [acc["id"] for acc in accounts]

        cur.execute(f"""
            UPDATE digital_accounts
            SET
                status = 'reserved',
                order_id = ?,
                reserved_at = ?
            WHERE id IN ({','.join('?' * len(ids))})
        """, [order_id, datetime.now(timezone.utc).isoformat(), *ids])

        conn.commit()
        return [dict(acc) for acc in accounts]

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_as_sold(order_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        UPDATE digital_accounts
        SET
            status = 'sold',
            sold_at = ?
        WHERE order_id = ?
    """, (datetime.now(timezone.utc).isoformat(), order_id))

    conn.commit()
    conn.close()


# --------------------
# DELIVERY
# --------------------
def deliver_digital_goods(order_id, item_id, accounts):
    url = f"{BASE_URL}/v2/campaigns/{CAMPAIGN_ID}/orders/{order_id}/deliverDigitalGoods"

    codes = []
    for acc in accounts:
        codes.append(
            f"Аккаунт:\n"
            f"Почта: {acc['login']}\n"
            f"Пароль от почты: {acc['password_mail']}\n"
            f"Пароль от ChatGPT: {acc['chatgpt_password']}\n"
            f"Имя: {acc['user_name']}"
        )

    payload = {
        "items": [
            {
                "id": item_id,
                "codes": codes,
                "slip": accounts[0]["instruction"],
                "activate_till": "01-01-2026"
            }
        ]
    }

    r = requests.post(url, json=payload, headers=HEADERS, timeout=30)
    r.raise_for_status()


# --------------------
# MAIN
# --------------------
def main():
    logging.info("Worker started")

    orders = get_orders()
    logging.info(f"Orders received: {len(orders)}")

    # 1️⃣ сохраняем статусы заказов
    for order in orders:
        upsert_order_status(order)

    # 2️⃣ обрабатываем только PROCESSING
    for order in orders:
        order_id = order.get("orderId")
        status = order.get("status")
        payment = order.get("paymentType")
        delivery = order.get("delivery", {}).get("type")

        if status != "PROCESSING":
            continue
        if payment != "PREPAID":
            continue
        if delivery != "DIGITAL":
            continue

        if order_already_processed(order_id):
            logging.info(f"Order {order_id} already processed, skip")
            continue

        try:
            logging.info(f"Processing order {order_id}")

            item = order["items"][0]
            item_id = item["id"]
            count = item.get("count", 1)

            accounts = reserve_accounts(order_id, count)
            if not accounts:
                logging.warning(
                    f"Not enough free accounts. Required={count}, orderId={order_id}"
                )
                continue

            deliver_digital_goods(order_id, item_id, accounts)
            mark_as_sold(order_id)

            logging.info(f"Order {order_id} delivered successfully")

        except Exception as e:
            logging.exception(f"Error processing order {order_id}: {e}")

    logging.info("Worker finished")


if __name__ == "__main__":
    main_loop()
