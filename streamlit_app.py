import streamlit as st
import duckdb

def check_and_run_auth():
    if st.secrets.auth.local_development:
        return
    if not st.experimental_user.is_logged_in:
        st.write("# RoboChargers BIG DATA")
        if st.button("Log in with Google"):
            st.login()
        st.stop()

    if st.button("Log out"):
        st.logout()
    st.markdown(f"Welcome! {st.experimental_user.name}")

check_and_run_auth()

# Define the pages
main_page = st.Page("main_page.py", title="Main Page", icon="🎈")
page_2 = st.Page("page_2.py", title="Coral Placement Timing", icon="📏")
page_3 = st.Page("page_3.py", title="Intake Metrics", icon="🐋")
page_4 = st.Page("page_4.py", title="WPILog Uploader", icon="⏫")

# Set up navigation
pg = st.navigation([main_page, page_2, page_3, page_4])


# Connect to DB
@st.cache_resource
def setup_duckdb() -> duckdb.DuckDBPyConnection:
    con: duckdb.DuckDBPyConnection = duckdb.connect("C:/Users/wtoth/frc.db")
    con.install_extension("s3")
    con.load_extension("s3")
    con.sql(f"""CREATE SECRET IF NOT EXISTS r2 (
                TYPE s3,
                KEY_ID '{st.secrets.parquet_r2.r2_key_id}',
                SECRET '{st.secrets.parquet_r2.r2_secret}',
                ENDPOINT '{st.secrets.parquet_r2.r2_endpoint}'
            );""")
    return con

st.session_state['conn'] = setup_duckdb()

# Run the selected page
pg.run()
