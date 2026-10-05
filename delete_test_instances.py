import asyncio
import sqlalchemy as sa
from app.db.session import create_engine
from app.core.config import get_settings


async def main():
    settings = get_settings()
    print("Banco:", settings.database_url[:60], "...")
    engine = create_engine()
    async with engine.begin() as conn:
        # Mostra quantas existem antes
        count = await conn.execute(
            sa.text("SELECT COUNT(*) FROM instances WHERE name LIKE 'test-instance-%'")
        )
        print(f"Test-instances encontradas: {count.scalar()}")

        r = await conn.execute(
            sa.text("DELETE FROM instances WHERE name LIKE 'test-instance-%' RETURNING name")
        )
        names = r.fetchall()
        print(f"Apagadas: {len(names)}")
        for row in names:
            print(" -", row[0])


asyncio.run(main())
