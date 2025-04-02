import streamlit as st
from query.column_selector import ColumnSelector
from query.sql_builder import SQLBuilder
from st_ant_tree import st_ant_tree
from sql_formatter.core import format_sql


st.markdown("# Coral Placement 🗞️")


st.sidebar.markdown("""# Coral Placement 🗞️
This metric tracks the time from when the laterator is commmanded to be out, to when the reef sensor is triggered.
""")

con = st.session_state['conn']


def getData():
  with open("query/coral_placement.sql", "r") as f:
      sql = f.read()
      return con.sql(sql).fetchdf()
  return None

df = getData()

# Print mean intake time for each event
for event, group in df[["shot_duration", "EventName"]].groupby("EventName"):
    stats = group["shot_duration"].describe()
    st.write(f"### {event}")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Mean", f"{stats['mean']:.2f}s", border=True)
    col2.metric("25%", f"{stats['25%']:.2f}s", border=True)
    col3.metric("75%", f"{stats['75%']:.2f}s", border=True)
    
    st.divider()


st.dataframe(
  df,
  column_config={
    "EventName": "Event",
    "MatchTime": "Match Time",
    "shot_duration": st.column_config.ProgressColumn(
        "Placement Time (s)", min_value=0, max_value=4, format="compact"
    ),
  },
  hide_index=True,
)

st.write(df[["shot_duration", "EventName"]].groupby("EventName").describe())
