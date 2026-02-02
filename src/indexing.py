"""
Indexing module for the Key-Value Store.
Provides secondary indexes, full-text search, and semantic search.
"""

import re
import math
from collections import defaultdict
from typing import Any, Dict, List, Set, Optional, Tuple


class ValueIndex:
    """
    Secondary index for value-based lookups.
    Indexes a specific field within JSON values.
    """
    
    def __init__(self, field: str):
        self.field = field
        # index[field_value] -> set of keys
        self.index: Dict[Any, Set[str]] = defaultdict(set)
    
    def add(self, key: str, value: Any) -> None:
        """Add a key-value pair to the index."""
        if isinstance(value, dict):
            field_value = value.get(self.field)
            if field_value is not None:
                self.index[field_value].add(key)
        elif self.field == "_value":
            # Special case: index the entire value
            self.index[value].add(key)
    
    def remove(self, key: str, value: Any) -> None:
        """Remove a key-value pair from the index."""
        if isinstance(value, dict):
            field_value = value.get(self.field)
            if field_value is not None and key in self.index[field_value]:
                self.index[field_value].discard(key)
                if not self.index[field_value]:
                    del self.index[field_value]
        elif self.field == "_value":
            if key in self.index.get(value, set()):
                self.index[value].discard(key)
                if not self.index[value]:
                    del self.index[value]
    
    def query(self, value: Any) -> List[str]:
        """Find all keys where field == value."""
        return list(self.index.get(value, set()))


class InvertedIndex:
    """
    Inverted index for full-text search.
    Tokenizes text values and maps words to document keys.
    """
    
    def __init__(self):
        # word -> {key: term_frequency}
        self.index: Dict[str, Dict[str, int]] = defaultdict(dict)
        # key -> document length (word count)
        self.doc_lengths: Dict[str, int] = {}
        # Total documents
        self.doc_count: int = 0
        # Average document length
        self.avg_doc_length: float = 0.0
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into lowercase words."""
        if not isinstance(text, str):
            text = str(text)
        # Remove punctuation and split on whitespace
        words = re.findall(r'\b\w+\b', text.lower())
        return words
    
    def _extract_text(self, value: Any) -> str:
        """Extract searchable text from a value."""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            # Concatenate all string values
            parts = []
            for v in value.values():
                if isinstance(v, str):
                    parts.append(v)
            return " ".join(parts)
        else:
            return str(value)
    
    def add(self, key: str, value: Any) -> None:
        """Index a document."""
        text = self._extract_text(value)
        words = self._tokenize(text)
        
        if not words:
            return
        
        # Update doc count
        if key not in self.doc_lengths:
            self.doc_count += 1
        
        # Term frequencies
        term_freq: Dict[str, int] = defaultdict(int)
        for word in words:
            term_freq[word] += 1
        
        # Update index
        for word, freq in term_freq.items():
            self.index[word][key] = freq
        
        # Update doc length
        self.doc_lengths[key] = len(words)
        
        # Update average
        if self.doc_count > 0:
            self.avg_doc_length = sum(self.doc_lengths.values()) / self.doc_count
    
    def remove(self, key: str) -> None:
        """Remove a document from the index."""
        if key not in self.doc_lengths:
            return
        
        # Remove from all word entries
        words_to_remove = []
        for word, docs in self.index.items():
            if key in docs:
                del docs[key]
                if not docs:
                    words_to_remove.append(word)
        
        for word in words_to_remove:
            del self.index[word]
        
        # Update counts
        del self.doc_lengths[key]
        self.doc_count -= 1
        
        if self.doc_count > 0:
            self.avg_doc_length = sum(self.doc_lengths.values()) / self.doc_count
        else:
            self.avg_doc_length = 0.0
    
    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Search for documents matching the query.
        Returns list of (key, score) sorted by relevance (BM25).
        """
        query_words = self._tokenize(query)
        if not query_words:
            return []
        
        scores: Dict[str, float] = defaultdict(float)
        
        # BM25 parameters
        k1 = 1.5
        b = 0.75
        
        for word in query_words:
            if word not in self.index:
                continue
            
            docs = self.index[word]
            # IDF
            idf = math.log((self.doc_count - len(docs) + 0.5) / (len(docs) + 0.5) + 1)
            
            for key, tf in docs.items():
                doc_len = self.doc_lengths.get(key, 1)
                # BM25 scoring
                numerator = tf * (k1 + 1)
                denominator = tf + k1 * (1 - b + b * doc_len / max(self.avg_doc_length, 1))
                scores[key] += idf * numerator / denominator
        
        # Sort by score descending
        results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return results[:top_k]


class EmbeddingIndex:
    """
    Semantic search using word embeddings.
    Uses simple TF-IDF vectors for document representation.
    """
    
    def __init__(self):
        # key -> vector (sparse: {word: weight})
        self.vectors: Dict[str, Dict[str, float]] = {}
        # IDF values computed from corpus
        self.idf: Dict[str, float] = {}
        # Document frequencies
        self.doc_freq: Dict[str, int] = defaultdict(int)
        self.doc_count: int = 0
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into lowercase words."""
        if not isinstance(text, str):
            text = str(text)
        return re.findall(r'\b\w+\b', text.lower())
    
    def _extract_text(self, value: Any) -> str:
        """Extract searchable text from a value."""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            parts = [str(v) for v in value.values() if isinstance(v, str)]
            return " ".join(parts)
        else:
            return str(value)
    
    def _compute_tf(self, words: List[str]) -> Dict[str, float]:
        """Compute term frequency with log normalization."""
        tf: Dict[str, float] = defaultdict(float)
        for word in words:
            tf[word] += 1
        # Log normalization
        for word in tf:
            tf[word] = 1 + math.log(tf[word])
        return dict(tf)
    
    def add(self, key: str, value: Any) -> None:
        """Add a document to the index."""
        text = self._extract_text(value)
        words = self._tokenize(text)
        
        if not words:
            return
        
        # Update doc count
        if key not in self.vectors:
            self.doc_count += 1
        else:
            # Remove old doc frequencies
            for word in self.vectors[key]:
                self.doc_freq[word] -= 1
        
        # Compute TF
        tf = self._compute_tf(words)
        
        # Update document frequencies
        unique_words = set(words)
        for word in unique_words:
            self.doc_freq[word] += 1
        
        # Store TF vector (IDF applied at query time)
        self.vectors[key] = tf
    
    def remove(self, key: str) -> None:
        """Remove a document from the index."""
        if key not in self.vectors:
            return
        
        # Update doc frequencies
        for word in self.vectors[key]:
            self.doc_freq[word] -= 1
            if self.doc_freq[word] <= 0:
                del self.doc_freq[word]
        
        del self.vectors[key]
        self.doc_count -= 1
    
    def _compute_tfidf(self, tf: Dict[str, float]) -> Dict[str, float]:
        """Apply IDF weighting to TF vector."""
        tfidf = {}
        for word, tf_val in tf.items():
            df = self.doc_freq.get(word, 0)
            if df > 0:
                idf = math.log(self.doc_count / df)
                tfidf[word] = tf_val * idf
        return tfidf
    
    def _cosine_similarity(self, v1: Dict[str, float], v2: Dict[str, float]) -> float:
        """Compute cosine similarity between two sparse vectors."""
        # Dot product
        dot = sum(v1.get(word, 0) * v2.get(word, 0) for word in set(v1) | set(v2))
        
        # Magnitudes
        mag1 = math.sqrt(sum(x * x for x in v1.values()))
        mag2 = math.sqrt(sum(x * x for x in v2.values()))
        
        if mag1 == 0 or mag2 == 0:
            return 0.0
        
        return dot / (mag1 * mag2)
    
    def semantic_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Find documents most similar to the query.
        Returns list of (key, similarity) sorted by similarity.
        """
        words = self._tokenize(query)
        if not words:
            return []
        
        # Compute query vector
        query_tf = self._compute_tf(words)
        query_vec = self._compute_tfidf(query_tf)
        
        # Compute similarities
        similarities = []
        for key, doc_tf in self.vectors.items():
            doc_vec = self._compute_tfidf(doc_tf)
            sim = self._cosine_similarity(query_vec, doc_vec)
            if sim > 0:
                similarities.append((key, sim))
        
        # Sort by similarity descending
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_k]


class IndexManager:
    """
    Manages all indexes for the KV store.
    """
    
    def __init__(self):
        self.value_indexes: Dict[str, ValueIndex] = {}
        self.inverted_index = InvertedIndex()
        self.embedding_index = EmbeddingIndex()
        self.fts_enabled = True  # Full-text search always enabled
    
    def create_value_index(self, field: str) -> None:
        """Create a secondary index on a field."""
        if field not in self.value_indexes:
            self.value_indexes[field] = ValueIndex(field)
    
    def on_set(self, key: str, value: Any, old_value: Any = None) -> None:
        """Called when a key is set. Updates all indexes."""
        # Update value indexes
        for idx in self.value_indexes.values():
            if old_value is not None:
                idx.remove(key, old_value)
            idx.add(key, value)
        
        # Update full-text index
        if old_value is not None:
            self.inverted_index.remove(key)
        self.inverted_index.add(key, value)
        
        # Update embedding index
        if old_value is not None:
            self.embedding_index.remove(key)
        self.embedding_index.add(key, value)
    
    def on_delete(self, key: str, value: Any) -> None:
        """Called when a key is deleted. Updates all indexes."""
        # Update value indexes
        for idx in self.value_indexes.values():
            idx.remove(key, value)
        
        # Update full-text index
        self.inverted_index.remove(key)
        
        # Update embedding index
        self.embedding_index.remove(key)
    
    def query_value_index(self, field: str, value: Any) -> List[str]:
        """Query a value index."""
        if field not in self.value_indexes:
            raise KeyError(f"No index on field '{field}'")
        return self.value_indexes[field].query(value)
    
    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Full-text search."""
        return self.inverted_index.search(query, top_k)
    
    def semantic_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic similarity search."""
        return self.embedding_index.semantic_search(query, top_k)
