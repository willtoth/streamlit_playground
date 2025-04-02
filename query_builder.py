import streamlit as st
from query.column_selector import ColumnSelector
from query.sql_builder import SQLBuilder
from st_ant_tree import st_ant_tree
from sql_formatter.core import format_sql


st.markdown("# Query Builder")
st.subheader("Build a Query")

st.sidebar.markdown("# Query Builder 🏗️")

con = st.session_state['conn']
selector = ColumnSelector(con)

table = "frc2025"

selection = selector.get_selection(table)
sqlBuilder = SQLBuilder(table)

sqlBuilder.add_columns(selection)
query = sqlBuilder.build()

def getData():
  return con.execute(f"{query}").fetchdf()

df = getData()

st.dataframe(df)

show_sql = st.checkbox("Show SQL")

if show_sql:
  st.code(format_sql(query), language="sql")
