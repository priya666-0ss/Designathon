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

def get_user_data(username: str) -> Dict[str, Any]:
    """
    Retrieve user data from PostgreSQL database based on username.
    """
    try:
        conn = get_db_connection()
        if not conn:
            return {"error": "Database connection failed"}

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get User Profile Data
            cur.execute("""
                SELECT e.employee_id, e.first_name, e.last_name, e.department, e.position
                FROM erp_employees e
                WHERE e.username = %s
            """, (username,))
            user_data = cur.fetchone()

            # Get Course Completion Data
            cur.execute("""
                SELECT 
                    c.course_name,
                    c.completion_date,
                    c.score
                FROM course_completions c
                WHERE c.username = %s
                ORDER BY c.completion_date DESC
            """, (username,))
            course_data = cur.fetchall()

            # Get Performance Ratings
            cur.execute("""
                SELECT 
                    p.rating_period,
                    p.overall_rating,
                    p.feedback
                FROM performance_ratings p
                WHERE p.username = %s
                ORDER BY p.rating_period DESC
            """, (username,))
            performance_data = cur.fetchall()

        conn.close()

        return {
            "user_data": dict(user_data) if user_data else None,
            "course_completions": [dict(row) for row in course_data],
            "performance_ratings": [dict(row) for row in performance_data]
        }

    except Exception as e:
        print(f"Error retrieving user data: {e}")
        return {"error": str(e)}

# Set of callable functions
profile_functions: Set[Callable[..., Any]] = {
    get_user_data,
}
