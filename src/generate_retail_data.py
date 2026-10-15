import psycopg2
import json
import time
import os
import random
import logging
from faker import Faker
from datetime import datetime
from dotenv import load_dotenv

# 1. Observability: Configure logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 2. Security: Load environment variables from the .env file
load_dotenv(dotenv_path='../.env')

fake = Faker()

# 3. Architecture: Medallion Architecture / Data Lake Zoning
# We strictly isolate raw, immutable events here.
RAW_DIR = '../data/raw'
os.makedirs(RAW_DIR, exist_ok=True)

def get_db_connection():
        """Establishes a connection to PostgreSQL reading credentials securely from local files."""
        
        # Read the secret directly from the text file
        try:
            with open('../secrets/db_password.txt', 'r') as f:
                db_password = f.read().strip()
        except FileNotFoundError:
            logger.error("FATAL: Secret file not found at ../secrets/db_password.txt")
            exit(1)

        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                database=os.getenv("DB_NAME", "retail_db"),
                user=os.getenv("DB_USER", "admin"),
                password=db_password
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"FATAL: Could not connect to PostgreSQL. Is the Docker container running? Error: {e}")
            exit(1)

def initialize_schema(conn):
    """Idempotent schema creation. Safe to run multiple times without destroying data."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    country VARCHAR(50),
                    created_at TIMESTAMP
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INT REFERENCES customers(customer_id),
                    product_category VARCHAR(50),
                    amount DECIMAL(10, 2),
                    order_time TIMESTAMP
                );
            """)
        conn.commit()
        logger.info("Database schema validated successfully.")
    except Exception as e:
        conn.rollback() # Always rollback on failure to prevent locked transactions
        logger.error(f"Schema initialization failed: {e}")
        raise

def generate_event_stream():
    conn = get_db_connection()
    initialize_schema(conn)
    
    # Bootstrap customers if empty
    customer_ids = []
    with conn.cursor() as cursor:
        for _ in range(50):
            cursor.execute(
                "INSERT INTO customers (name, email, country, created_at) VALUES (%s, %s, %s, %s) RETURNING customer_id",
                (fake.name(), fake.email(), fake.country(), datetime.now())
            )
            customer_ids.append(cursor.fetchone()[0])
    conn.commit()
    logger.info("Seeded 50 master customer records.")

    logger.info("Starting real-time transactional simulation. Press Ctrl+C to interrupt.")
    
    try:
        while True:
            cust_id = random.choice(customer_ids)
            category = random.choice(['Electronics', 'Apparel', 'Home', 'Logistics_Freight'])
            amount = round(random.uniform(15.0, 1500.0), 2)
            
            # We use single inserts here specifically to simulate a high-frequency OLTP web backend.
            # In a batch processing scenario, we would use execute_batch() for performance.
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO orders (customer_id, product_category, amount, order_time) VALUES (%s, %s, %s, %s)",
                        (cust_id, category, amount, datetime.now())
                    )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.warning(f"Failed to insert order for customer {cust_id}: {e}")
            
            # Generate Semi-Structured Logistics Log (Immutable Raw Zone)
            logistics_event = {
                "event_id": fake.uuid4(),
                "customer_id": cust_id,
                "status": random.choice(['Dispatched', 'In_Transit', 'Delivered', 'Delayed']),
                "location_lat": float(fake.latitude()),
                "location_lon": float(fake.longitude()),
                "timestamp": datetime.now().isoformat()
            }
            
            log_file_path = os.path.join(RAW_DIR, 'delivery_logs.json')
            with open(log_file_path, 'a') as f:
                f.write(json.dumps(logistics_event) + '\n')
                
            logger.info(f"Processed Order & Logistics Event | Customer: {cust_id} | Category: {category}")
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated by user.")
    finally:
        conn.close()
        logger.info("Database connections closed.")

if __name__ == "__main__":
    generate_event_stream()