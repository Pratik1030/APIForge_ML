
import asyncio
import os

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

async def main():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("ERROR: DATABASE_URL was not found in .env")
        return

    engine = create_async_engine(database_url)

    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT 1"))
            print("Supabase connection successful:", result.scalar_one())
    except Exception as error:
        print("Database connection failed.")
        print(type(error).__name__ + ":", str(error))
    finally:
        await engine.dispose()

asyncio.run(main())
