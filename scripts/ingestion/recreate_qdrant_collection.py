from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

# Conexión local (ajusta si usas Qdrant Cloud)
client = QdrantClient(host="localhost", port=6333)

# Recrear colección con dimensión 768
client.recreate_collection(
    collection_name="rag_collection",
    vectors_config=VectorParams(
        size=768,
        distance=Distance.COSINE  # Puedes usar EUCLID o DOT si prefieres
    )
)

print("Colección 'rag_collection' recreada con dimensión 768.")