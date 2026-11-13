"""Hashtag and keyword extraction utilities for social media events."""

from __future__ import annotations

import re

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"@[\w.-]+")
HASHTAG_IN_TEXT_RE = re.compile(r"#([\w\d_À-ỹ]+)", re.UNICODE)
WORD_RE = re.compile(r"[\wÀ-ỹ]{3,}", re.UNICODE)
# Letters that only exist in Vietnamese: their presence marks a Vietnamese text even without `lang`.
VIETNAMESE_RE = re.compile(
    r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]", re.IGNORECASE
)
# Vietnamese words are mostly two syllables ("sản phẩm", "làn da"), so phrases are built inside
# runs of letters only: punctuation and digits end a phrase.
PHRASE_SPLIT_RE = re.compile(r"[^\w\s]|\d|_", re.UNICODE)
SYLLABLE_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

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
    "sau", "trước", "năm", "tháng", "tuần", "hôm", "nay", "giờ", "mới", "cùng", "tới",
    "luôn", "thật", "chỉ", "đều", "nhau", "vừa", "hết", "cả", "mọi", "ai", "gì", "này", "kìa",
    # Vietnamese unaccented common words (often used in loadgen / social comments)
    "cua", "nhung", "mot", "nhieu", "trong", "tren", "voi", "cho", "khi", "nay",
    "thi", "duoc", "nhu", "khong", "chua", "nguoi", "minh", "ban", "chung", "anh",
    "xem", "biet", "noi", "thay", "muon", "can", "phai", "roi", "nhe", "nha",
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
    # Fragments left by contractions ("don't" -> "don") and bare domains ("x.com" -> "com")
    "don", "doesn", "didn", "isn", "aren", "wasn", "weren", "won", "couldn", "shouldn",
    "wouldn", "haven", "hasn", "com", "org", "net", "www", "http", "https", "amp",
    "really", "even", "still", "much", "want", "know", "think", "going", "being", "way",
})


def normalize_tag(raw: str) -> str:
    """Clean and normalize a hashtag or keyword (strip leading #, lowercase)."""
    cleaned = raw.strip().lstrip("#").lower()
    return cleaned


def is_vietnamese(event: dict, text: str) -> bool:
    lang = str(event.get("lang") or event.get("default_language") or "").lower()
    return lang.startswith("vi") or bool(VIETNAMESE_RE.search(text))


def vietnamese_keywords(text: str) -> list[str]:
    """Two-syllable phrases for Vietnamese, where single syllables ("cảm", "tiết") mean nothing.

    Plain ASCII words of 4+ letters (brand and loan words such as "iphone", "review") are kept
    as single keywords too.
    """
    def loanword(word: str) -> bool:
        return word.isascii() and len(word) >= 4

    keywords: list[str] = []
    for chunk in PHRASE_SPLIT_RE.split(text):
        syllables = [s.lower() for s in SYLLABLE_RE.findall(chunk)]
        for i, syllable in enumerate(syllables):
            if loanword(syllable) and syllable not in STOPWORDS:
                keywords.append(syllable)
            if i + 1 < len(syllables):
                nxt = syllables[i + 1]
                if syllable in STOPWORDS or nxt in STOPWORDS or len(syllable) < 2 or len(nxt) < 2:
                    continue
                # "chi tiết iphone" must not yield "tiết iphone": a loanword pairs only with ASCII.
                if (loanword(syllable) and not nxt.isascii()) or (loanword(nxt) and not syllable.isascii()):
                    continue
                keywords.append(f"{syllable} {nxt}")
    return keywords


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

    if is_vietnamese(event, cleaned_text):
        for phrase in vietnamese_keywords(cleaned_text):
            if phrase not in results:
                results[phrase] = False
            if len(results) >= max_keywords_per_event:
                break
        return list(results.items())[:max_keywords_per_event]

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
