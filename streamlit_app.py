import streamlit as st
import duckdb
import os

# def check_and_run_auth():
#     if st.secrets.auth.local_development:
#         return
#     if not st.experimental_user.is_logged_in:
#         st.write("# RoboChargers BIG DATA")
#         if st.button("Log in with Google"):
#             st.login()
#         st.stop()

#     if st.button("Log out"):
#         st.logout()
#     st.markdown(f"Welcome! {st.experimental_user.name}")

# def get_username() -> str | None:
#     if st.secrets.auth.local_development:
#         return "local"
#     if not st.experimental_user.is_logged_in:
#         return None
#     return st.experimental_user.email.split("@")[0]

# def get_database_path() -> str | None:
#     username = get_username()
#     if not username:
#         return None

#     return f"./{username}.db"

# def provision_user():
#     username = get_username()

#     if not username:
#         st.write("Invalid user")
#         st.stop()

#     if not os.path.exists(f"./{username}.db"):
#         do_provision_database()

# def do_provision_database():
#     status = st.write("Provisioning database, can take a while...")
#     database_path = get_database_path()

#     if not database_path:
#         st.write("Invalid database path")
#         st.stop()

#     con: duckdb.DuckDBPyConnection = duckdb.connect(database_path, read_only=True)
#     con.install_extension("s3")
#     con.load_extension("s3")
#     con.sql(f"""CREATE SECRET IF NOT EXISTS r2 (
#                 TYPE s3,
#                 KEY_ID '{st.secrets.parquet_r2.r2_key_id}',
#                 SECRET '{st.secrets.parquet_r2.r2_secret}',
#                 ENDPOINT '{st.secrets.parquet_r2.r2_endpoint}'
#             );""")
    
#     with open("query/dbinit.sql", "r") as f:
#         init_sql = f.read()
#         con.sql(init_sql)
    
#     status = st.success("Database provisioned successfully!")
#     con.close()

# check_and_run_auth()

def get_database_path():
    return "C:\\Users\\wtoth\\frc.db"

# Define the pages
main_page = st.Page("main_page.py", title="Main Page", icon="🎈")
page_2 = st.Page("page_2.py", title="Coral Placement Timing", icon="📏")
page_3 = st.Page("page_3.py", title="Intake Metrics", icon="🐋")
page_4 = st.Page("page_4.py", title="WPILog Uploader", icon="⏫")
query_builder = st.Page("query_builder.py", title="Query Builder", icon="🚧")

# Set up navigation
pg = st.navigation([main_page, page_2, page_3, page_4])

# Connect to DB
@st.cache_resource
def setup_duckdb() -> duckdb.DuckDBPyConnection:
    # provision_user()
    
    con: duckdb.DuckDBPyConnection = duckdb.connect(get_database_path(), read_only=True)
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
