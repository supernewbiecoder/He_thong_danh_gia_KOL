"""Hashtag and keyword extraction utilities for social media events."""

from __future__ import annotations

import re

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"@[\w.-]+")
HASHTAG_IN_TEXT_RE = re.compile(r"#([\w\d_À-ỹ]+)", re.UNICODE)
WORD_RE = re.compile(r"[\wÀ-ỹ]{3,}", re.UNICODE)

# Common Vietnamese & English stopwords to filter out noise when extracting keywords from text
STOPWORDS: frozenset[str] = frozenset({
    # Vietnamese
    "của", "và", "các", "những", "một", "nhiều", "trong", "ngoài", "trên", "dưới",
    "với", "cho", "khi", "này", "kia", "đó", "đây", "ấy", "thì", "mà", "là", "được",
    "bị", "do", "bởi", "vì", "nên", "nếu", "nhưng", "hoặc", "hay", "cũng", "đã",
    "đang", "sẽ", "vẫn", "còn", "rất", "quá", "lắm", "hơn", "nhất", "không", "chưa",
    "chẳng", "cách", "việc", "người", "mình", "bạn", "tôi", "chúng", "họ", "nó",
    "anh", "chị", "em", "ông", "bà", "cái", "chiếc", "như", "theo", "từ", "đến",
    "tại", "về", "qua", "lại", "ra", "vào", "lên", "xuống", "đi", "làm", "có",
    "xem", "biết", "nói", "thấy", "muốn", "cần", "phải", "thể", "rồi", "nhé", "nha",
    "ạ", "ơi", "vậy", "sao", "nào", "đâu", "bao", "nhiêu", "mấy", "lúc", "ngày",
    # Vietnamese unaccented common words (often used in loadgen / social comments)
    "cua", "nhung", "mot", "nhieu", "trong", "tren", "voi", "cho", "khi", "nay",
    "thi", "duoc", "nhu", "khong", "chua", "nguoi", "minh", "ban", "chung", "anh",
    "chi", "xem", "biet", "noi", "thay", "muon", "can", "phai", "roi", "nhe", "nha",
    # English
    "the", "and", "for", "that", "this", "with", "you", "not", "are", "from",
    "your", "all", "have", "new", "more", "an", "was", "we", "will", "home",
    "can", "us", "about", "if", "page", "my", "has", "search", "free", "but",
    "our", "one", "other", "do", "no", "information", "time", "they", "site",
    "he", "up", "may", "what", "which", "their", "news", "out", "use", "any",
    "there", "see", "only", "so", "his", "when", "contact", "here", "business",
    "who", "web", "also", "now", "help", "get", "pm", "view", "online", "first",
    "am", "been", "would", "how", "were", "me", "services", "some", "these",
    "click", "its", "like", "service", "than", "find", "price", "date", "back",
    "top", "people", "had", "list", "name", "just", "over", "state", "year",
    "day", "into", "email", "two", "health", "world", "re", "next", "used",
    "go", "work", "last", "most", "products", "music", "buy", "data", "make",
    "them", "should", "product", "system", "post", "her", "city", "add", "policy",
    "number", "such", "please", "available", "copyright", "support", "message",
    "after", "best", "software", "then", "jan", "good", "video", "well", "where",
    "info", "rights", "public", "books", "high", "school", "through", "each",
    "links", "she", "review", "years", "order", "very", "privacy", "book", "items",
    "company", "read", "group", "need", "many", "user", "said", "de", "does",
    "set", "under", "general", "research", "university", "mail", "full",
})


def normalize_tag(raw: str) -> str:
    """Clean and normalize a hashtag or keyword (strip leading #, lowercase)."""
    cleaned = raw.strip().lstrip("#").lower()
    return cleaned


def extract_keywords_and_hashtags(
    event: dict,
    extract_from_text: bool = True,
    max_keywords_per_event: int = 15,
) -> list[tuple[str, bool]]:
    """Return a deduplicated list of (keyword, is_hashtag) pairs from an event.

    Explicit hashtags from `event["hashtags"]` and `#tag` tokens inside `event["text"]`
    are marked with `is_hashtag=True`. Significant words from `event["text"]` are marked
    with `is_hashtag=False`.
    """
    results: dict[str, bool] = {}

    # 1. Explicit hashtags and platform tags fields from schema
    for field_name in ("hashtags", "tags"):
        raw_list = event.get(field_name)
        if isinstance(raw_list, list):
            for item in raw_list:
                if isinstance(item, str):
                    tag = normalize_tag(item)
                    if len(tag) >= 2 and not tag.isdigit():
                        results[tag] = True

    # Also check facets of type "tag" (from Bluesky Jetstream)
    facets = event.get("facets")
    if isinstance(facets, list):
        for facet in facets:
            if isinstance(facet, dict) and facet.get("type") == "tag":
                val = facet.get("value")
                if isinstance(val, str):
                    tag = normalize_tag(val)
                    if len(tag) >= 2 and not tag.isdigit():
                        results[tag] = True

    text = event.get("text")
    if not isinstance(text, str) or not text.strip():
        return list(results.items())[:max_keywords_per_event]

    # 2. Inline #hashtags in text
    for match in HASHTAG_IN_TEXT_RE.finditer(text):
        tag = normalize_tag(match.group(1))
        if len(tag) >= 2 and not tag.isdigit():
            results[tag] = True

    if not extract_from_text:
        return list(results.items())[:max_keywords_per_event]

    # 3. Clean URLs, @mentions, and #hashtags before extracting plain words
    cleaned_text = URL_RE.sub(" ", text)
    cleaned_text = MENTION_RE.sub(" ", cleaned_text)
    cleaned_text = HASHTAG_IN_TEXT_RE.sub(" ", cleaned_text)

    for match in WORD_RE.finditer(cleaned_text):
        word = match.group(0).lower().strip("_")
        if len(word) < 3 or len(word) > 32 or word.isdigit():
            continue
        if word in STOPWORDS:
            continue
        # Do not overwrite if already registered as a hashtag
        if word not in results:
            results[word] = False
        if len(results) >= max_keywords_per_event:
            break

    return list(results.items())[:max_keywords_per_event]
