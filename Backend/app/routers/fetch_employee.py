from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import asyncpg

# Initialize FastAPI app
app = FastAPI()

# Database connection settings
DATABASE_URL = "postgresql://username:password@localhost:5432/your_database"

# Pydantic model for response
class Employee(BaseModel):
    id: int
    name: str
    position: str
    department: str

# Dependency to connect to the database
async def get_db_connection():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

@app.get("/employee/{employee_id}", response_model=Employee)
async def get_employee(employee_id: int):
    """
    Fetch employee details by ID from PostgreSQL database.
    """
    conn = None
    try:
        # Get database connection
        conn = await get_db_connection()

        # Query the database for the employee
        query = "SELECT id, name, position, department FROM employees WHERE id = $1"
        employee = await conn.fetchrow(query, employee_id)

        # If employee not found, raise 404 error
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        # Return employee details as JSON
        return {
            "id": employee["id"],
            "name": employee["name"],
            "position": employee["position"],
            "department": employee["department"],
        }

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions to be handled by FastAPI
        raise http_exc

    except Exception as e:
        # Catch any unexpected errors and return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

    finally:
        # Ensure the database connection is closed
        if conn:
            await conn.close()
