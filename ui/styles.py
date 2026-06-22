from __future__ import annotations

import streamlit as st

APP_STYLES = """
<style>
div[class*="st-key-cluster-expander-"] details > summary p,
div[class*="st-key-search-expander-"] details > summary p {
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    gap: 1rem;
}
div[class*="st-key-cluster-expander-"] details > summary p strong,
div[class*="st-key-search-expander-"] details > summary p strong {
    font-weight: 600;
}
div[class*="st-key-cluster-expander-"] details > summary p,
div[class*="st-key-search-expander-"] details > summary p {
    color: #8b949e;
    font-size: 0.9em;
}
div[class*="st-key-cluster-expander-"] details > summary p strong,
div[class*="st-key-search-expander-"] details > summary p strong {
    color: #f0f2f6;
    font-size: 1.1em;
}
div[class*="st-key-cluster-expander-"] details > summary p a,
div[class*="st-key-search-expander-"] details > summary p a {
    color: inherit;
    text-decoration: none;
}
div[class*="st-key-read-more-"] button {
    color: #58a6ff;
    padding: 0;
    min-height: auto;
    border: 0;
    background: transparent;
}
div[class*="st-key-read-more-"] button:hover {
    color: #79c0ff;
    background: transparent;
}
.sidebar-profile {
    color: #f0f2f6;
    font-size: 1.05rem;
    font-weight: 600;
    line-height: 1.5rem;
    margin: 0;
}
.sidebar-profile span {
    color: #8b949e;
    font-size: 0.9rem;
    font-weight: 400;
}
.sidebar-organization {
    color: #8b949e;
    font-size: 0.85rem;
    font-weight: 400;
    line-height: 1.25rem;
    margin-top: 0.1rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.organization-balance {
    color: #8b949e;
    font-size: 0.85rem;
    white-space: nowrap;
}
.organization-balance strong {
    color: #d7dbe0;
    font-size: 0.95rem;
    font-weight: 600;
}
div.st-key-refresh-balance button {
    padding: 0.15rem 0.45rem;
    min-height: auto;
}
div.st-key-refresh-balance {
    display: flex;
    justify-content: flex-end;
    align-items: center;
    height: 100%;
    padding-top: 0.8rem;
}
[class*="st-key-import-news-file"]
[data-testid="stFileUploaderDropzone"] button p {
    font-size: 0;
}
[class*="st-key-import-news-file"]
[data-testid="stFileUploaderDropzone"] button p::after {
    content: "Выбрать файл";
    font-size: 0.875rem;
}
div.st-key-sidebar-logout button {
    min-height: auto;
    padding: 0.15rem 0.25rem;
    color: #8b949e;
    font-size: 0.8rem;
    white-space: nowrap;
}
div.st-key-sidebar-logout {
    display: flex;
    justify-content: flex-start;
    align-items: center;
    min-height: 1.5rem;
    margin-top: 0.75rem;
}
@media (min-width: 769px) {
    section[data-testid="stSidebar"] {
        width: 17.25rem !important;
        min-width: 17.25rem !important;
        max-width: 17.25rem !important;
    }
    section[data-testid="stSidebar"] > div {
        width: 17.25rem !important;
    }
}
</style>
"""


def apply_styles() -> None:
    st.markdown(APP_STYLES, unsafe_allow_html=True)
