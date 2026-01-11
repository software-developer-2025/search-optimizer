import json
import psycopg2
from google import genai

# ---------- CONFIG ----------
DB_URL = "postgresql:purple-base-ahmewhvt-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

client = genai.Client()

COLOR_FAMILY = {
    "Red": ["Maroon", "Burgundy", "Crimson"],
    "Blue": ["Navy Blue", "Sky Blue"],
    "Black": ["Charcoal"],
    "White": ["Off White"]
}
# ----------------------------


# 1️⃣ Gemini → Natural Language → Structured Query
def parse_query_with_gemini(user_query: str) -> dict:
    prompt = f"""
You are a query parser for an ecommerce search system.

Convert the user query into structured JSON.
Return ONLY valid JSON. No explanation.

Allowed keys:
- category
- subcategory
- color

User query:
"{user_query}"

Example output:
{{
  "category": "Bag",
  "color": "Red"
}}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return json.loads(response.text)
    except Exception:
        return {}


# 2️⃣ Ranked SQL Search with Pagination
def search_products(parsed_query: dict, page: int, page_size: int):
    offset = page * page_size
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    filters = []
    params = []

    if "category" in parsed_query:
        filters.append("category ILIKE %s")
        params.append(f"%{parsed_query['category']}%")

    if "subcategory" in parsed_query:
        filters.append("subcategory ILIKE %s")
        params.append(f"%{parsed_query['subcategory']}%")

    where_clause = " AND ".join(filters)
    if where_clause:
        where_clause = "WHERE " + where_clause

    color = parsed_query.get("color")
    color_family = COLOR_FAMILY.get(color, [])

    sql = f"""
    SELECT product_id, name, category, subcategory, color, image_url
    FROM products_search
    {where_clause}
    ORDER BY
        CASE
            WHEN color = %s THEN 1
            WHEN color = ANY(%s) THEN 2
            ELSE 3
        END,
        product_id
    LIMIT %s OFFSET %s;
    """

    cur.execute(
        sql,
        params + [color, color_family, page_size, offset]
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows


# 3️⃣ Public API for UI
def search(user_query: str, page: int, page_size: int):
    parsed = parse_query_with_gemini(user_query) if page == 0 else None
    results = search_products(parsed or {}, page, page_size)
    return parsed, results
