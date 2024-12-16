from sqlalchemy import create_engine

# Database connection URI
DATABASE_URI = "postgresql+psycopg2://postgres:yourpassword@localhost:5432/timeseries_db"

# Create engine
engine = create_engine(DATABASE_URI)

def get_db_connection():
    return engine.connect()
