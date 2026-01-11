import streamlit as st
from search_engine import search

st.set_page_config(page_title="Product Search", layout="wide")
st.title("🛍️ Product Search Engine")

PAGE_SIZE = 24

# ---- Session State ----
if "page" not in st.session_state:
    st.session_state.page = 0

if "query" not in st.session_state:
    st.session_state.query = ""

if "results" not in st.session_state:
    st.session_state.results = []

# ---- Search Input ----
query = st.text_input(
    "Search products",
    placeholder="red bag, party wear dress, casual shoes"
)

# ---- New Search ----
if query and query != st.session_state.query:
    st.session_state.query = query
    st.session_state.page = 0
    st.session_state.results = []

    parsed, new_results = search(query, 0, PAGE_SIZE)
    st.session_state.results.extend(new_results)

    st.subheader("🔍 Interpreted Query")
    st.json(parsed)

# ---- Display Results ----
if st.session_state.results:
    st.subheader("🧾 Products")

    cols = st.columns(4)
    for i, product in enumerate(st.session_state.results):
        with cols[i % 4]:
            st.image(product[5], use_container_width=True)
            st.markdown(f"**{product[1]}**")
            st.caption(f"{product[2]} | {product[3]} | {product[4]}")

    # ---- Load More ----
    if st.button("Load more"):
        st.session_state.page += 1
        _, more_results = search(
            st.session_state.query,
            st.session_state.page,
            PAGE_SIZE
        )
        st.session_state.results.extend(more_results)
