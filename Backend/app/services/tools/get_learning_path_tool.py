import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, Set, Callable

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        connection = psycopg2.connect(
            dbname='db_pls',
            user='db_pls_user',
            password='password123',
            host='127.0.0.1',
            port='5432'
        )
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_learning_path(username: str) -> Dict[str, Any]:
    """
    Retrieve the learning path name for a user from the PostgreSQL database.
    
    Args:
        username (str): The username of the user.
    
    Returns:
        Dict[str, Any]: A dictionary containing the user's learning path name.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return {"error": "Database connection failed"}

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT learning_path_name
                FROM learning_paths 
                WHERE username = %s
            """, (username,))
            learning_path_data = cur.fetchone()

        conn.close()

        if not learning_path_data:
            return {"error": "No learning path found for the user"}

        return {
            "learning_path_name": learning_path_data["learning_path_name"]
        }

    except Exception as e:
        print(f"Error retrieving learning path name: {e}")
        return {"error": str(e)}


# Set of callable functions
assesment_functions: Set[Callable[..., Any]] = {
    get_learning_path,
}
