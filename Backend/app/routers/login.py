from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from core.jwt_auth import sign_jwt  # Assuming you have a JWT utility for signing tokens
import logging

# Initialize logger
logger = logging.getLogger("auth_logger")
logger.setLevel(logging.INFO)

# Define the router
auth_router = APIRouter(prefix="/api/auth", tags=["Auth"])

# Database connection details
DB_CONFIG = {
    "dbname": "db_pls",
    "user": "db_pls_user",
    "password": "password123",
    "host": "127.0.0.1",
    "port": "5432",
}

# Pydantic model for login request
class UserLogin(BaseModel):
    username: str
    password: str

# Function to fetch user details from the database
def fetch_user_from_db(username: str):
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = "SELECT * FROM users WHERE username = %s"
        cursor.execute(query, (username,))
        user = cursor.fetchone()
        cursor.close()
        connection.close()
        return user
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection error",
        )

# Login endpoint
@auth_router.post("/login")
def login(client: UserLogin):
    try:
        username = client.username
        password = client.password
        logger.info(f"Login attempt for username: {username}")

        # Fetch user from the database
        user = fetch_user_from_db(username)

        if user is None:
            logger.warning(f"User not found: {username}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found",
            )

        # Verify password (in production, use hashed passwords)
        if user["password"] == password:
            # Generate JWT token
            token = sign_jwt(username)
            result = {
                "status": "success",
                "message": "Login successful",
                "token": token,
            }
            logger.info(f"Login successful for username: {username}")
            return JSONResponse(status_code=status.HTTP_200_OK, content=result)
        else:
            logger.warning(f"Invalid credentials for username: {username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
            )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error during login: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred",
        )
