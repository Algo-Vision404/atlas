try:
    import torch
    from sentence_transformers import SentenceTransformer
except ImportError:
    torch = None
    SentenceTransformer = None
import logging
from typing import List, Union
from libs.core.config import settings

logger = logging.getLogger("atlas.embedding")

class EmbeddingEngine:
    def __init__(self, model_name: str = settings.EMBEDDING_MODEL):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Loading embedding model: {model_name} on {self.device}")
        self.model = SentenceTransformer(model_name, device=self.device)

    def encode(self, text: Union[str, List[str]]) -> List[float]:
        """Convert text or list of texts to embeddings"""
        try:
            embeddings = self.model.encode(text, convert_to_numpy=True)
            if isinstance(text, str):
                return embeddings.tolist()
            return embeddings.tolist()
        except Exception as e:
            logger.error(f"Embedding failed: {str(e)}")
            raise

# Worker implementation for distributed embedding
class EmbeddingWorker:
    def __init__(self):
        self.engine = EmbeddingEngine()

    async def process_batch(self, texts: List[str]) -> List[List[float]]:
        # In a real system, this would be triggered by a Kafka event
        # and would process batches of extracted text
        return self.engine.encode(texts)

if __name__ == "__main__":
    # Test
    logging.basicConfig(level=logging.INFO)
    engine = EmbeddingEngine()
    test_text = "Distributed web intelligence is the future of search."
    vec = engine.encode(test_text)
    print(f"Vector size: {len(vec)}")
    print(f"First 5 dimensions: {vec[:5]}")
