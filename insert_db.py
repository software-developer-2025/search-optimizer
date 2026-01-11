import pandas as pd
import psycopg2

# ---------------- CONFIG ----------------
STYLES_CSV = "styles.csv"
IMAGES_CSV = "images.csv"
TOTAL_ROWS = 850
RANDOM_SEED = 42

CATEGORY_TARGETS = {
    "Apparel": 450,
    "Footwear": 300,
    "Personal Care": 100
}

DB_CONFIG = "postgresql://neondb_owner:npg_Ua8rFN9dHSgX@ep-purple-base-ahmewhvt-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
# ---------------------------------------


# 1️⃣ Load CSVs
styles = pd.read_csv(STYLES_CSV, on_bad_lines="skip")
images = pd.read_csv(IMAGES_CSV)

# 2️⃣ Clean styles
styles = styles[
    [
        "id",
        "gender",
        "masterCategory",
        "subCategory",
        "articleType",
        "baseColour",
        "productDisplayName"
    ]
]

styles = styles.dropna(
    subset=["id", "articleType", "baseColour", "productDisplayName"]
)

# 3️⃣ Prepare images mapping
images["id"] = images["filename"].str.replace(".jpg", "", regex=False).astype(int)
images = images[["id", "link"]]

# 4️⃣ Join styles + images
df = styles.merge(images, on="id", how="inner")

# 5️⃣ Normalize text
for col in ["articleType", "subCategory", "baseColour", "gender", "masterCategory"]:
    df[col] = df[col].astype(str).str.strip().str.title()

# 6️⃣ EXCLUDE Accessories (already inserted)
df = df[df["masterCategory"] != "Accessories"]

# 7️⃣ Category-wise controlled selection
final_rows = []

for category, count in CATEGORY_TARGETS.items():
    subset = df[df["masterCategory"] == category]
    subset = subset.sample(frac=1, random_state=RANDOM_SEED)
    final_rows.append(subset.head(count))

final_df = pd.concat(final_rows).head(TOTAL_ROWS)

# 8️⃣ Insert into Neon Postgres
conn = psycopg2.connect(DB_CONFIG)
cur = conn.cursor()

insert_sql = """
INSERT INTO products_search (
    product_id, name, brand, category,
    subcategory, color, gender, image_url
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (product_id) DO NOTHING;
"""

for _, row in final_df.iterrows():
    cur.execute(
        insert_sql,
        (
            int(row["id"]),
            row["productDisplayName"],
            None,
            row["articleType"],
            row["subCategory"],
            row["baseColour"],
            row["gender"],
            row["link"]
        )
    )

conn.commit()
cur.close()
conn.close()

print(f"Inserted {len(final_df)} leftover products successfully.")
