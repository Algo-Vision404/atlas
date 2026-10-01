import hashlib
import logging
import math
from typing import List, Optional, Union

try:
    import torch
    from sentence_transformers import SentenceTransformer
except ImportError:
    torch = None
    SentenceTransformer = None

from libs.core.config import settings

logger = logging.getLogger("atlas.embedding")


class EmbeddingEngine:
    def __init__(
        self,
        model_name: str = settings.EMBEDDING_MODEL,
        dimension: int = settings.EMBEDDING_DIMENSION,
    ):
        self.model_name = model_name
        self.dimension = dimension
        self.device = "cuda" if (torch is not None and torch.cuda.is_available()) else "cpu"
        self.model: Optional[object] = None

    def _ensure_model(self) -> Optional[object]:
        if settings.MOCK_MODE:
            return None
        if self.model is not None:
            return self.model
        if SentenceTransformer is None:
            logger.warning(
                "sentence-transformers is not installed; falling back to deterministic embeddings"
            )
            return None

        logger.info("Loading embedding model: %s on %s", self.model_name, self.device)
        self.model = SentenceTransformer(self.model_name, device=self.device)
        model_dim = getattr(self.model, "get_sentence_embedding_dimension", lambda: None)()
        if isinstance(model_dim, int) and model_dim > 0:
            self.dimension = model_dim
        return self.model

    def _mock_vector(self, text: str) -> List[float]:
        raw: List[float] = []
        counter = 0
        while len(raw) < self.dimension:
            digest = hashlib.sha256(f"{text}:{counter}".encode("utf-8")).digest()
            for byte in digest:
                raw.append((byte / 127.5) - 1.0)
                if len(raw) == self.dimension:
                    break
            counter += 1
        norm = math.sqrt(sum(v * v for v in raw)) or 1.0
        return [round(v / norm, 6) for v in raw]

    def encode(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """Convert text or list of texts to embeddings."""
        model = self._ensure_model()
        if model is None:
            if isinstance(text, str):
                return self._mock_vector(text)
            return [self._mock_vector(item) for item in text]

        try:
            embeddings = model.encode(text, convert_to_numpy=True)
            return embeddings.tolist()
        except Exception as e:
            logger.error("Embedding failed: %s", str(e))
            raise


class EmbeddingWorker:
    def __init__(self):
        self.engine = EmbeddingEngine()

    async def process_batch(self, texts: List[str]) -> List[List[float]]:
        return self.engine.encode(texts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    engine = EmbeddingEngine()
    test_text = "Distributed web intelligence is the future of search."
    vec = engine.encode(test_text)
    print(f"Vector size: {len(vec)}")
    print(f"First 5 dimensions: {vec[:5]}")
