import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database configuration
DATABASE_URI = "postgresql+psycopg2://postgres:yourpassword@localhost:5432/timeseries_db"
engine = create_engine(DATABASE_URI)

def get_db_connection():
    return engine.connect()

def create_hypertable():
    try:
        with get_db_connection() as conn:
            # Start a transaction
            with conn.begin():
                # Create table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        time TIMESTAMPTZ NOT NULL,
                        sensor_id INT NOT NULL,
                        temperature DOUBLE PRECISION,
                        humidity DOUBLE PRECISION
                    );
                """))
                # Convert to hypertable
                conn.execute(text("SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);"))
                logging.info("Hypertable created.")
    except SQLAlchemyError as e:
        logging.error(f"Error creating hypertable: {e}")


def insert_sample_data():
    try:
        with get_db_connection() as conn:
            with conn.begin():
                conn.execute(text("""
                    INSERT INTO sensor_data (time, sensor_id, temperature, humidity)
                    VALUES
                    (NOW(), 1, 22.5, 60.1),
                    (NOW() - INTERVAL '1 hour', 1, 21.9, 61.2),
                    (NOW() - INTERVAL '2 hours', 2, 23.1, 59.8);
                """))
                logging.info("Sample data inserted.")
    except SQLAlchemyError as e:
        logging.error(f"Error inserting sample data: {e}")



def query_aggregated_data():
    try:
        with get_db_connection() as conn:
            # Example query: Average temperature grouped by sensor_id
            result = conn.execute(text("""
                SELECT
                    sensor_id,
                    AVG(temperature) AS avg_temperature,
                    MAX(humidity) AS max_humidity
                FROM sensor_data
                GROUP BY sensor_id;
            """))

            # Print results
            logging.info("Aggregated Data:")
            for row in result:
                logging.info(f"Sensor ID: {row.sensor_id}, Avg Temp: {row.avg_temperature}, Max Humidity: {row.max_humidity}")
    except SQLAlchemyError as e:
        logging.error(f"Error querying data: {e}")



if __name__ == "__main__":
    create_hypertable()
    insert_sample_data()
    query_aggregated_data()
