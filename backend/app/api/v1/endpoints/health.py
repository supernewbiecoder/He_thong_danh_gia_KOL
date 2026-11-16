from fastapi import APIRouter

from ....core.database import get_connection

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    with get_connection() as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}
