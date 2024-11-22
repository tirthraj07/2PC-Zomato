from db import MySQLDatabase
import os
from flask import Flask, jsonify, request
import requests

from dotenv import load_dotenv
load_dotenv()

## LOADING ENVIRONMENT VARIABLES
PARTNER_SERVICE_PORT = os.getenv("PARTNER_SERVICE_PORT")
PRODUCT_SERVICE_PORT = os.getenv("PRODUCT_SERVICE_PORT")
BOOKING_SERVICE_PORT = os.getenv("BOOKING_SERVICE_PORT")

PARTNER_SERVICE_URL = f"http://localhost:{PARTNER_SERVICE_PORT}"
PRODUCT_SERVICE_URL = f"http://localhost:{PRODUCT_SERVICE_PORT}"
BOOKING_SERVICE_URL = f"http://localhost:{BOOKING_SERVICE_PORT}"

DB_HOST = os.getenv("DB_HOST")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

## Database Connectivity

db = MySQLDatabase(host=DB_HOST, user=DB_USERNAME, password=DB_PASSWORD, database=DB_NAME)
db.connect()
db.execute_update("""
CREATE TABLE IF NOT EXISTS bookings (
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    partner_id INT NOT NULL
);
""")


app = Flask(__name__)

## APP ROUTES

@app.route('/book', methods=['POST'])
def home_route():
    USER_ID = request.args.get("user_id", type=int)
    PRODUCT_NAME = request.args.get("product_name", type=str)
 
    if not USER_ID or not PRODUCT_NAME:
        return jsonify({"error": "user_id and product_name are required"}), 400
    
     # Phase 1: Reserve Product
    try:
        data = {"product_name": PRODUCT_NAME}
        response = requests.post(f"{PRODUCT_SERVICE_URL}/reserve", json=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        return jsonify({"error": f"Couldn't reserve product: {str(e)}"}), 500

    # Phase 1: Reserve Partner
    try:
        response = requests.post(f"{PARTNER_SERVICE_URL}/reserve", json=data)
        response.raise_for_status()
    except requests.RequestException as e:
        return jsonify({"error": f"Couldn't reserve partner: {str(e)}"}), 500

    # Phase 2: Book Product
    try:
        response = requests.post(f"{PRODUCT_SERVICE_URL}/book", json=data)
        response.raise_for_status()
        PRODUCT_ID = response.json()["product_id"]
    except requests.RequestException as e:
        return jsonify({"error": f"Couldn't book product: {str(e)}"}), 500

    # Phase 2: Book Partner
    try:
        response = requests.post(f"{PARTNER_SERVICE_URL}/book", json=data)
        response.raise_for_status()
        PARTNER_ID = response.json()["partner_id"]
    except requests.RequestException as e:
        return jsonify({"error": f"Couldn't book partner: {str(e)}"}), 500

    # Database Entry
    try:
        db.execute_update(
            "INSERT INTO bookings (user_id, product_id, partner_id) VALUES (%s, %s, %s);",
            (USER_ID, PRODUCT_ID, PARTNER_ID)
        )
    except Exception as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

    return jsonify({
        "success": f"Item with ID: {PRODUCT_ID} and Delivery partner: {PARTNER_ID} successfully booked"
    }), 201

# Teardown
@app.teardown_appcontext
def close_db_connection(exception):
    if db:
        try:
            db.close()
        except Exception as e:
            app.logger.warning(f"Error closing database: {e}")

if __name__ == '__main__':
    app.run(port=BOOKING_SERVICE_PORT, debug=True)