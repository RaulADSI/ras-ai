"""
Script: embed_gl_directory.py
Purpose: Generate embeddings for GL accounts using MiniLM
Model: all-MiniLM-L6-v2
Vector size: 768
Date: 2025-09-25
Author: Rulo
"""

import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
import json

# --- Load CSV ---
df = pd.read_csv("data/clean/normalized_gl_accounts.csv")

# --- Validate if required columns are present ---
required_cols = {"normalized_name", "gl_code"}
missing_cols = required_cols - set(df.columns)
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

# Fill missing values
df["normalized_name"] = df["normalized_name"].fillna("").astype(str).str.strip()
df["gl_code"] = df["gl_code"].fillna("").astype(str).str.strip()

# --- Load model ---
model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Generate embeddings (you probably only want normalized_name) ---
embeddings = model.encode(df["normalized_name"].tolist(), show_progress_bar=True)

# --- Save as .npy ---
np.save("data/embedded/gl_directory_embeddings.npy", embeddings)

# --- Reload vectors ---
vectors = np.load("data/embedded/gl_directory_embeddings.npy")

# --- Build Qdrant points ---
points = []
for idx, row in df.iterrows():
    payload = {
        "normalized_name": row["normalized_name"],
        "gl_code": row["gl_code"]
    }
    if "raw_name" in df.columns:
        payload["raw_name"] = row["raw_name"]

    point = {
        "id": str(idx),
        "vector": vectors[idx].tolist(),
        "payload": payload
    }
    points.append(point)

# --- Save as JSON for Qdrant (optional export) ---
with open("data/embedded/gl_points.json", "w", encoding="utf-8") as f:
    json.dump({"points": points}, f, ensure_ascii=False, indent=2)
