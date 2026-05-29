"""
의약품 허가정보 API (DrugPrdtPrmsnInfoService07)
→ medicine_products (목록 API) + product_ingredients (주성분 API) 동기화

사용: python scripts/sync_products.py [--probe] [--limit N]
  --probe: API 응답 필드 확인
  --limit N: 최대 N건만 수집 (테스트용)
"""
import asyncio
import httpx
import uuid
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import os
from sqlalchemy import select
from app.database import SessionLocal, engine, Base
from app.models import medicine as _m, interaction as _i  # noqa: F401
from app.models.medicine import MedicineProduct, ActiveIngredient, ProductIngredient

DUR_API_KEY = os.environ.get("DUR_API_KEY", "")
BASE_URL = "https://apis.data.go.kr/1471000/DrugPrdtPrmsnInfoService07"


async def fetch_page(client: httpx.AsyncClient, endpoint: str,
                     page: int, num_rows: int = 100) -> dict:
    params = {
        "serviceKey": DUR_API_KEY,
        "pageNo": page,
        "numOfRows": num_rows,
        "type": "json",
    }
    resp = await client.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def parse_body(data: dict) -> tuple[list[dict], int]:
    body = data.get("body", {})
    total = int(body.get("totalCount", 0))
    items = body.get("items", [])
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        items = []
    return items, total


async def sync_products(db, client: httpx.AsyncClient, limit: int | None = None) -> int:
    """목록 API로 medicine_products 동기화"""
    endpoint = "getDrugPrdtPrmsnInq07"
    first_data = await fetch_page(client, endpoint, 1)
    items, total = parse_body(first_data)
    if not items:
        print("제품 데이터 없음")
        return 0

    if limit:
        total = min(total, limit)
    pages = (total + 99) // 100
    print(f"의약품 제품 목록: 총 {total}건, {pages}페이지")

    added = 0

    async def process_batch(batch: list[dict]) -> int:
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

    added += await process_batch(items[:limit] if limit else items)
    collected = len(items)

    for page in range(2, pages + 1):
        if limit and collected >= limit:
            break
        data = await fetch_page(client, endpoint, page)
        items, _ = parse_body(data)
        if not items:
            break
        if limit:
            items = items[:limit - collected]
        added += await process_batch(items)
        collected += len(items)
        if page % 50 == 0:
            print(f"  제품 {page}/{pages} 페이지 | {added}건 추가")

    return added


async def sync_ingredients(db, client: httpx.AsyncClient, limit: int | None = None) -> dict:
    """주성분 API로 product_ingredients 동기화"""
    endpoint = "getDrugPrdtMcpnDtlInq07"
    first_data = await fetch_page(client, endpoint, 1)
    items, total = parse_body(first_data)
    if not items:
        print("주성분 데이터 없음")
        return {"links": 0, "new_ingredients": 0}

    if limit:
        total = min(total, limit)
    pages = (total + 99) // 100
    print(f"주성분 정보: 총 {total}건, {pages}페이지")

    stats = {"links": 0, "new_ingredients": 0}

    async def process_batch(batch: list[dict]):
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

            # 성분 찾기/생성
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

            # 중복 연결 방지
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
            stats["links"] += 1
        await db.commit()

    await process_batch(items[:limit] if limit else items)
    collected = len(items)

    for page in range(2, pages + 1):
        if limit and collected >= limit:
            break
        data = await fetch_page(client, endpoint, page)
        items, _ = parse_body(data)
        if not items:
            break
        if limit:
            items = items[:limit - collected]
        await process_batch(items)
        collected += len(items)
        if page % 50 == 0:
            print(f"  주성분 {page}/{pages} 페이지 | 연결 {stats['links']}건")

    return stats


async def probe():
    if not DUR_API_KEY:
        print("DUR_API_KEY가 설정되지 않았습니다.")
        return
    async with httpx.AsyncClient() as client:
        for ep, label in [
            ("getDrugPrdtPrmsnInq07", "목록"),
            ("getDrugPrdtMcpnDtlInq07", "주성분"),
        ]:
            data = await fetch_page(client, ep, 1, 2)
            items, total = parse_body(data)
            print(f"\n=== [{label}] {ep} (총 {total}건) ===")
            if items:
                for k, v in items[0].items():
                    val = str(v)[:80] if v else "(비어있음)"
                    print(f"  {k}: {val}")


async def main():
    if not DUR_API_KEY:
        print("오류: DUR_API_KEY 환경변수가 없습니다.")
        sys.exit(1)

    if "--probe" in sys.argv:
        await probe()
        return

    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with httpx.AsyncClient() as client:
        async with SessionLocal() as db:
            print("=== 1단계: 제품 목록 동기화 ===")
            products_added = await sync_products(db, client, limit)
            print(f"제품 {products_added}건 추가\n")

            print("=== 2단계: 주성분 연결 동기화 ===")
            ingr_stats = await sync_ingredients(db, client, limit)
            print(f"성분 연결 {ingr_stats['links']}건, 신규 성분 {ingr_stats['new_ingredients']}건\n")

    print("완료!")


asyncio.run(main())
