from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
import httpx
import uuid
import logging

from app.database import get_db, SessionLocal
from app.config import settings
from app.models.medicine import ActiveIngredient
from app.models.interaction import FoodItem, SupplementIngredient, InteractionRule

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

DUR_BASE_URL = "http://apis.data.go.kr/1471000/DURPrdlstInfoService03"


async def _fetch_page(client: httpx.AsyncClient, page: int, num_rows: int = 100) -> dict:
    params = {
        "serviceKey": settings.dur_api_key,
        "pageNo": page,
        "numOfRows": num_rows,
        "type": "json",
    }
    resp = await client.get(f"{DUR_BASE_URL}/getUsjntTabooInfoList03", params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def _get_or_create_ingredient(db: AsyncSession, code: str, name_ko: str) -> ActiveIngredient:
    row = (await db.execute(
        select(ActiveIngredient).where(ActiveIngredient.ingredient_code == code)
    )).scalar_one_or_none()
    if not row:
        row = ActiveIngredient(
            id=str(uuid.uuid4()),
            ingredient_name_ko=name_ko or code,
            ingredient_code=code,
        )
        db.add(row)
        await db.flush()
    return row


async def _process_page(db: AsyncSession, items: list) -> int:
    added = 0
    for item in items:
        ingr_code = item.get("INGR_CODE", "")
        ingr_name = item.get("INGR_NAME", "") or item.get("INGR_KOR_NAME", "")
        mix_code = item.get("MIXTURE_INGR", "") or item.get("MIXTURE_INGR_CODE", "")
        mix_name = item.get("MIXTURE_INGR_KOR_NAME", "") or item.get("MIXTURE_INGR_NAME", "") or mix_code
        reason = item.get("PROHBT_CONTENT", "") or item.get("REMARK", "")

        if not (ingr_code and mix_code):
            continue

        subject = await _get_or_create_ingredient(db, ingr_code, ingr_name)
        obj = await _get_or_create_ingredient(db, mix_code, mix_name)

        exists = (await db.execute(
            select(InteractionRule).where(
                InteractionRule.subject_id == subject.id,
                InteractionRule.object_id == obj.id,
                InteractionRule.interaction_type == "contraindication",
            )
        )).scalar_one_or_none()

        if not exists:
            db.add(InteractionRule(
                id=str(uuid.uuid4()),
                subject_type="drug",
                subject_id=subject.id,
                object_type="drug",
                object_id=obj.id,
                interaction_type="contraindication",
                severity="critical",
                mechanism=reason or None,
                recommendation="이 약물 조합은 병용금기입니다. 반드시 의사·약사와 상담하세요.",
                evidence_source="식약처 DUR 병용금기",
                is_active=True,
            ))
            added += 1
    await db.commit()
    return added


async def _run_sync_dur():
    added = 0
    total_fetched = 0
    try:
        async with httpx.AsyncClient() as client:
            first = await _fetch_page(client, 1, num_rows=50)
            body = first.get("body", {})
            total = int(body.get("totalCount", 0))
            pages = (total + 49) // 50
            logger.info("DUR 동기화 시작: 총 %d건, %d페이지", total, pages)

            items = body.get("items", [])
            if isinstance(items, dict):
                items = [items]

            async with SessionLocal() as db:
                total_fetched += len(items)
                added += await _process_page(db, items)

                for page in range(2, pages + 1):
                    data = await _fetch_page(client, page, num_rows=50)
                    items = data.get("body", {}).get("items", [])
                    if isinstance(items, dict):
                        items = [items]
                    elif not isinstance(items, list):
                        items = []
                    total_fetched += len(items)
                    added += await _process_page(db, items)
                    if page % 100 == 0:
                        logger.info("DUR 동기화 진행 중: %d/%d 페이지", page, pages)

        logger.info("DUR 동기화 완료: %d건 저장, %d건 수집", added, total_fetched)
    except Exception:
        logger.exception("DUR 동기화 중 오류 발생")


@router.post("/sync-dur", status_code=202)
async def sync_dur(background_tasks: BackgroundTasks):
    if not settings.dur_api_key:
        raise HTTPException(status_code=400, detail="DUR_API_KEY가 설정되지 않았습니다.")
    background_tasks.add_task(_run_sync_dur)
    return {"message": "DUR 동기화 시작됨. /api/v1/admin/stats 로 진행 상황 확인 가능."}


@router.get("/probe-dur")
async def probe_dur():
    """DUR API 첫 번째 항목의 실제 필드명과 값 확인용"""
    if not settings.dur_api_key:
        raise HTTPException(status_code=400, detail="DUR_API_KEY가 설정되지 않았습니다.")
    async with httpx.AsyncClient() as client:
        data = await _fetch_page(client, 1, num_rows=1)
    body = data.get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict):
        items = [items]
    return {
        "totalCount": body.get("totalCount"),
        "first_item": items[0] if items else None,
    }


@router.get("/stats")
async def stats(db: AsyncSession = Depends(get_db)):
    return {
        "ingredients": (await db.execute(select(func.count(ActiveIngredient.id)))).scalar(),
        "food_items": (await db.execute(select(func.count(FoodItem.id)))).scalar(),
        "supplements": (await db.execute(select(func.count(SupplementIngredient.id)))).scalar(),
        "interaction_rules": (await db.execute(select(func.count(InteractionRule.id)))).scalar(),
    }
