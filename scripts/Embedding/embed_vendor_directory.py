import pandas as pd
import numpy as np
import json
from sentence_transformers import SentenceTransformer

# --- Load and clean ---
df = pd.read_csv("data/clean/normalized_vendor_directory.csv")
df["normalized_company"] = df["normalized_company"].fillna("").astype(str).str.strip()

# --- Load model ---
model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Generate embeddings ---
embeddings = model.encode(df["normalized_company"].tolist(), show_progress_bar=True)

# --- Save vectors ---
np.save("data/embedded/vendor_embeddings.npy", embeddings)

# --- Build Qdrant points ---
points = []
for idx, row in df.iterrows():
    payload = {
        "normalized_company": row["normalized_company"],
        "company_name": row.get("company_name", "")
    }
    points.append({
        "id": str(idx),
        "vector": embeddings[idx].tolist(),
        "payload": payload
    })

# --- Export JSON ---
with open("data/embedded/vendor_points.json", "w", encoding="utf-8") as f:
    json.dump({"points": points}, f, ensure_ascii=False, indent=2)