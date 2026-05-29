from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
import httpx
import uuid
import json
import logging
from pathlib import Path

from app.database import get_db, SessionLocal
from app.config import settings
from app.models.medicine import ActiveIngredient, MedicineProduct, ProductIngredient
from app.models.interaction import FoodItem, SupplementIngredient, InteractionRule

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

DUR_BASE_URL = "http://apis.data.go.kr/1471000/DURPrdlstInfoService03"
SEEDS_DIR = Path(__file__).resolve().parent.parent / "data" / "seeds"


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
        "products": (await db.execute(select(func.count(MedicineProduct.id)))).scalar(),
        "product_ingredients": (await db.execute(select(func.count(ProductIngredient.id)))).scalar(),
        "food_items": (await db.execute(select(func.count(FoodItem.id)))).scalar(),
        "supplements": (await db.execute(select(func.count(SupplementIngredient.id)))).scalar(),
        "interaction_rules": (await db.execute(select(func.count(InteractionRule.id)))).scalar(),
    }


# --- 제품 동기화 (DrugPrdtPrmsnInfoService07 API) ---

PRODUCT_API_URL = "https://apis.data.go.kr/1471000/DrugPrdtPrmsnInfoService07"


async def _fetch_product_page(client: httpx.AsyncClient, endpoint: str,
                               page: int, num_rows: int = 100) -> dict:
    params = {
        "serviceKey": settings.dur_api_key,
        "pageNo": page,
        "numOfRows": num_rows,
        "type": "json",
    }
    resp = await client.get(f"{PRODUCT_API_URL}/{endpoint}", params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def _parse_items(data: dict) -> tuple[list[dict], int]:
    body = data.get("body", {})
    total = int(body.get("totalCount", 0))
    items = body.get("items", [])
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        items = []
    return items, total


async def _run_sync_products():
    stats = {"products_added": 0, "links_added": 0, "new_ingredients": 0}
    try:
        async with httpx.AsyncClient() as client:
            # 1단계: 제품 목록 (getDrugPrdtPrmsnInq07)
            logger.info("제품 목록 동기화 시작")
            first = await _fetch_product_page(client, "getDrugPrdtPrmsnInq07", 1)
            items, total = _parse_items(first)
            pages = (total + 99) // 100
            logger.info("제품 총 %d건, %d페이지", total, pages)

            async with SessionLocal() as db:
                async def save_products(batch: list[dict]):
                    count = 0
                    for item in batch:
                        item_seq = str(item.get("ITEM_SEQ", "") or "")
                        item_name = item.get("ITEM_NAME", "") or ""
                        if not item_seq or not item_name:
                            continue
                        exists = (await db.execute(
                            select(MedicineProduct).where(MedicineProduct.item_seq == item_seq)
                        )).scalar_one_or_none()
                        if exists:
                            continue
                        db.add(MedicineProduct(
                            id=str(uuid.uuid4()),
                            product_name=item_name,
                            manufacturer=item.get("ENTP_NAME", "") or None,
                            item_seq=item_seq,
                            dosage_form=item.get("PRDUCT_TYPE", "") or None,
                            source="DrugPrdtPrmsnInfoService07",
                        ))
                        count += 1
                    await db.commit()
                    return count

                stats["products_added"] += await save_products(items)
                for page in range(2, pages + 1):
                    data = await _fetch_product_page(client, "getDrugPrdtPrmsnInq07", page)
                    items, _ = _parse_items(data)
                    if not items:
                        break
                    stats["products_added"] += await save_products(items)
                    if page % 50 == 0:
                        logger.info("제품 %d/%d 페이지 | %d건 추가", page, pages, stats["products_added"])

            # 2단계: 주성분 연결 (getDrugPrdtMcpnDtlInq07)
            logger.info("주성분 연결 동기화 시작")
            first = await _fetch_product_page(client, "getDrugPrdtMcpnDtlInq07", 1)
            items, total = _parse_items(first)
            pages = (total + 99) // 100
            logger.info("주성분 총 %d건, %d페이지", total, pages)

            async with SessionLocal() as db:
                async def save_ingredients(batch: list[dict]):
                    for item in batch:
                        item_seq = str(item.get("ITEM_SEQ", "") or "")
                        mtral_code = item.get("MTRAL_CODE", "") or ""
                        mtral_name = item.get("MTRAL_NM", "") or ""
                        if not item_seq or not (mtral_code or mtral_name):
                            continue

                        product = (await db.execute(
                            select(MedicineProduct).where(MedicineProduct.item_seq == item_seq)
                        )).scalar_one_or_none()
                        if not product:
                            continue

                        ingr = None
                        if mtral_code:
                            ingr = (await db.execute(
                                select(ActiveIngredient).where(ActiveIngredient.ingredient_code == mtral_code)
                            )).scalar_one_or_none()
                        if not ingr and mtral_name:
                            ingr = (await db.execute(
                                select(ActiveIngredient).where(ActiveIngredient.ingredient_name_ko == mtral_name)
                            )).scalar_one_or_none()
                        if not ingr:
                            ingr = ActiveIngredient(
                                id=str(uuid.uuid4()),
                                ingredient_name_ko=mtral_name or mtral_code,
                                ingredient_code=mtral_code or None,
                            )
                            db.add(ingr)
                            await db.flush()
                            stats["new_ingredients"] += 1

                        exists = (await db.execute(
                            select(ProductIngredient).where(
                                ProductIngredient.product_id == product.id,
                                ProductIngredient.ingredient_id == ingr.id,
                            )
                        )).scalar_one_or_none()
                        if exists:
                            continue

                        qnt = item.get("QNT")
                        amount = None
                        if qnt:
                            try:
                                amount = float(str(qnt).replace(",", ""))
                            except ValueError:
                                pass

                        db.add(ProductIngredient(
                            id=str(uuid.uuid4()),
                            product_id=product.id,
                            ingredient_id=ingr.id,
                            amount=amount,
                            unit=item.get("INGD_UNIT_CD") or None,
                            is_main=True,
                        ))
                        stats["links_added"] += 1
                    await db.commit()

                await save_ingredients(items)
                for page in range(2, pages + 1):
                    data = await _fetch_product_page(client, "getDrugPrdtMcpnDtlInq07", page)
                    items, _ = _parse_items(data)
                    if not items:
                        break
                    await save_ingredients(items)
                    if page % 50 == 0:
                        logger.info("주성분 %d/%d 페이지 | 연결 %d건", page, pages, stats["links_added"])

        logger.info("제품 동기화 완료: 제품 %d건, 연결 %d건, 신규성분 %d건",
                     stats["products_added"], stats["links_added"], stats["new_ingredients"])
    except Exception:
        logger.exception("제품 동기화 중 오류 발생")


@router.post("/sync-products", status_code=202)
async def sync_products(background_tasks: BackgroundTasks):
    """의약품 허가정보 API로 medicine_products + product_ingredients 동기화"""
    if not settings.dur_api_key:
        raise HTTPException(status_code=400, detail="DUR_API_KEY가 설정되지 않았습니다.")
    background_tasks.add_task(_run_sync_products)
    return {"message": "제품 동기화 시작됨 (제품 4.3만건 + 주성분 12.7만건). /api/v1/admin/stats 로 확인 가능."}


# --- 시드 데이터 로딩 (food_items, supplement_ingredients) ---

@router.post("/load-seeds", status_code=200)
async def load_seeds(db: AsyncSession = Depends(get_db)):
    """food_items.json, supplements.json을 DB에 로딩"""
    result = {}

    food_file = SEEDS_DIR / "food_items.json"
    if food_file.exists():
        food_data = json.loads(food_file.read_text(encoding="utf-8"))
        added = 0
        for item in food_data:
            exists = (await db.execute(
                select(FoodItem).where(FoodItem.food_name == item["food_name"])
            )).scalar_one_or_none()
            if not exists:
                db.add(FoodItem(id=str(uuid.uuid4()), **item))
                added += 1
        result["food_items"] = f"{added}건 추가"

    supp_file = SEEDS_DIR / "supplements.json"
    if supp_file.exists():
        supp_data = json.loads(supp_file.read_text(encoding="utf-8"))
        added = 0
        for item in supp_data:
            exists = (await db.execute(
                select(SupplementIngredient).where(SupplementIngredient.name_ko == item["name_ko"])
            )).scalar_one_or_none()
            if not exists:
                db.add(SupplementIngredient(id=str(uuid.uuid4()), **item))
                added += 1
        result["supplements"] = f"{added}건 추가"

    await db.commit()
    return result
