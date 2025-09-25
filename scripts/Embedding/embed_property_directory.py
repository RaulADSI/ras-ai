import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
import pandas as pd
import numpy as np
import json

# --- Load CSV ---
df = pd.read_csv("data/clean/normalized_property_directory.csv")

# --- Validate if column is present ---
if "normalized_property" not in df.columns:
    raise ValueError("The column 'normalized_property' doesn't exist.")

# --- Preprocessing ---
df["normalized_property"] = df["normalized_property"].fillna("").astype(str).str.strip()

# --- Load model ---
model = SentenceTransformer("all-MiniLM-L6-v2")  

# --- Generate vectors ---
embeddings = model.encode(df["normalized_property"].tolist(), show_progress_bar=True)

# --- save as .npy ---
np.save("data/embedded/property_directory_embeddings.npy", embeddings)

# --- Opcional: save as CSV with IDs ---
embedding_df = pd.DataFrame(embeddings)
embedding_df["normalized_property"] = df["normalized_property"]
embedding_df["raw_property"] = df["raw_property"]
embedding_df.to_csv("data/embedded/property_directory_embeddings.csv", index=False)

# --- Load data and vectors ---
df = pd.read_csv("data/clean/normalized_property_directory.csv")
vectors = np.load("data/embedded/property_directory_embeddings.npy")

# Build Qdrant points
points = []
for idx, row in df.iterrows():
    point = {
        "id": str(idx),  # puede ser un UUID si prefieres
        "vector": vectors[idx].tolist(),
        "payload": {
            "raw_property": row["raw_property"],
            "normalized_property": row["normalized_property"]
        }
    }
    points.append(point)

# --- Save as JSON for Qdrant ---
with open("data/embedded/property_points.json", "w", encoding="utf-8") as f:
    json.dump({"points": points}, f, ensure_ascii=False, indent=2)