from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import SearchRequest

model = SentenceTransformer("all-MiniLM-L6-v2")
client = QdrantClient(host="localhost", port=6333)

query = "11515 NW 22nd Avenue - BLDG 2 - 11515 NW 22nd Avenue Miami, FL 33167"
vector = model.encode(query).tolist()

results = client.search(
    collection_name="rag_collection",
    query_vector=vector,
    limit=3,
    with_payload=True
)

for r in results:
    print(f"Score: {r.score:.3f} → {r.payload.get('normalized_property')}")