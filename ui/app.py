"""Streamlit dashboard for the read-only KOL API."""

from __future__ import annotations

import os
from typing import Any

import requests
import streamlit as st

API_URL = os.getenv("KOL_API_URL", "http://127.0.0.1:8000").rstrip("/")
PLATFORMS = {"Tất cả": "all", "Bluesky": "bluesky", "YouTube": "youtube"}
PERIODS = {"15 phút": 15}

st.set_page_config(page_title="Xếp hạng KOL", page_icon="📊", layout="wide")
st.markdown(
    """
    <style>
    .block-container {max-width: 1400px; padding-top: 2rem;}
    .kpi {background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:16px 18px;}
    .kpi-label {color:#64748b;font-size:.85rem}.kpi-value {color:#0f1f3d;font-size:1.65rem;font-weight:800}
    .stApp {background:#f8fafc}.stButton button {border-radius:8px}
    </style>
    """,
    unsafe_allow_html=True,
)


def api_get(path: str, **params: Any) -> dict[str, Any] | list[dict[str, Any]]:
    try:
        response = requests.get(f"{API_URL}{path}", params=params, timeout=8)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        st.error(f"API không kết nối được: {exc}")
        st.stop()


def number(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{int(value or 0):,}".replace(",", ".")


@st.cache_data(ttl=10)
def load_summary(period: int, platform: str) -> dict[str, Any]:
    return api_get("/api/v1/dashboard/summary", period=period, platform=platform)  # type: ignore[return-value]


@st.cache_data(ttl=10)
def load_kols(period: int, platform: str, search: str, sort: str) -> dict[str, Any]:
    return api_get("/api/v1/dashboard/kols", period=period, platform=platform, search=search, sort=sort, limit=50)  # type: ignore[return-value]


@st.cache_data(ttl=10)
def load_keywords(period: int, search: str) -> dict[str, Any]:
    return api_get("/api/v1/dashboard/keywords", period=period, search=search, limit=30)  # type: ignore[return-value]


st.title("Xếp hạng KOL và từ khóa thịnh hành")
st.caption(f"Dữ liệu đọc từ FastAPI: `{API_URL}`")

col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    period_label = st.selectbox("Kỳ thống kê", list(PERIODS))
with col2:
    platform_label = st.selectbox("Nền tảng", list(PLATFORMS))
with col3:
    search = st.text_input("Tìm KOL hoặc từ khóa", placeholder="Nhập tên hoặc ID KOL")

period = PERIODS[period_label]
platform = PLATFORMS[platform_label]
summary = load_summary(period, platform)

st.subheader("Tổng quan")
k1, k2, k3, k4 = st.columns(4)
for col, label, value in (
    (k1, "Tổng số KOL", number(summary["kols"])),
    (k2, "Tổng sự kiện", number(summary["events"])),
    (k3, "Điểm tương tác", number(summary["engagement_score"])),
    (k4, "Tốc độ điểm/phút", number(summary["velocity_per_min"])),
):
    col.markdown(
        f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div></div>',
        unsafe_allow_html=True,
    )

st.divider()
left, right = st.columns([1.55, 1])
with left:
    st.subheader("KOL nổi bật trên social media")
    sort_label = st.selectbox(
        "Sắp xếp theo",
        ["Điểm tương tác", "Điểm cửa sổ", "Tốc độ điểm/phút", "Tổng sự kiện"],
    )
    sort = {
        "Điểm tương tác": "engagement_score",
        "Điểm cửa sổ": "window_score",
        "Tốc độ điểm/phút": "velocity_per_min",
        "Tổng sự kiện": "total_events",
    }[sort_label]
    kol_data = load_kols(period, platform, search, sort)
    items = kol_data["items"]
    if items:
        table = [
            {
                "Hạng": i + 1,
                "KOL": row.get("handle") or row["kol_id"],
                "Nền tảng": row["platform"],
                "Điểm": round(row["engagement_score"], 2),
                "Điểm cửa sổ": round(row["window_score"], 2),
                "Điểm/phút": round(row["velocity_per_min"], 2),
                "Sự kiện": row["total_events"],
            }
            for i, row in enumerate(items)
        ]
        st.dataframe(table, use_container_width=True, hide_index=True)
        st.bar_chart({row.get("handle") or row["kol_id"]: row["engagement_score"] for row in items[:10]})
    else:
        st.info("Chưa có dữ liệu KOL phù hợp.")

with right:
    st.subheader("Từ khóa thịnh hành")
    keyword_search = st.text_input("Lọc từ khóa", key="keyword_search")
    keyword_data = load_keywords(period, keyword_search)
    keyword_items = keyword_data["items"]
    if keyword_items:
        st.dataframe(
            [
                {
                    "Từ khóa": row["keyword"],
                    "Lượt dùng": row["count"],
                    "KOL duy nhất": row["unique_kols"],
                    "Tốc độ/phút": round(row["velocity_per_min"], 2),
                }
                for row in keyword_items
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.bar_chart({row["keyword"]: row["count"] for row in keyword_items[:10]})
    else:
        st.info("Chưa có dữ liệu từ khóa.")

st.subheader("Chi tiết KOL")
if items:
    choices = {f'{row.get("handle") or row["kol_id"]} ({row["platform"]})': row["kol_id"] for row in items}
    selected_label = st.selectbox("Chọn KOL", list(choices))
    selected = api_get(f"/api/v1/dashboard/kols/{choices[selected_label]}")
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Điểm tương tác", number(selected["engagement_score"]))
    d2.metric("Điểm cửa sổ", number(selected["window_score"]))
    d3.metric("Người theo dõi", number(selected.get("followers") or 0))
    d4.metric("Tổng sự kiện", number(selected["total_events"]))
    history = api_get(f"/api/v1/dashboard/kols/{choices[selected_label]}/history", days=7)
    if history["items"]:
        st.line_chart(
            {
                "Điểm tổng": {row["bucket"]: row["engagement_score"] for row in history["items"]},
                "Điểm cửa sổ": {row["bucket"]: row["window_score"] for row in history["items"]},
            }
        )
