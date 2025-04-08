import streamlit as st
import plotly.express as px
from sql_formatter.core import format_sql

from query import action_breakdown


st.markdown("# Robot Action Breakdown 🤖")


st.sidebar.markdown("""# Robot Action Breakdown 🤖

""")

con = st.session_state['conn']

@st.cache_data
def getData():
    sql = action_breakdown.all()
    return con.sql(sql).fetchdf()

df = getData()
st.session_state['robot_action_table'] = df

# Create consistent color mapping
unique_actions = df['RobotAction'].dropna().unique()
color_palette = px.colors.qualitative.Alphabet  # or choose another like Safe, Bold, etc.
color_map = {action: color_palette[i % len(color_palette)] for i, action in enumerate(sorted(unique_actions))}
st.session_state['robot_action_colors'] = color_map

SECONDS_PER_ROW = 0.02

for event, group in df[["RobotAction", "EventName", "MatchKey", "MatchTime"]].groupby("EventName"):
    st.write(f"### {event}")
  
    # Count actions
    action_counts = group['RobotAction'].value_counts().reset_index()
    action_counts.columns = ['RobotAction', 'Count']
    action_counts['Seconds'] = action_counts['Count'] * SECONDS_PER_ROW

    # Create pie chart
    fig = px.pie(action_counts,
                 names='RobotAction',
                 values='Seconds',
                 title=f'Action Distribution for {event}',
                 color='RobotAction',
                 color_discrete_map=color_map
    )
    
    fig.update_traces(textinfo='label+percent', hovertemplate='%{label}: %{value:.2f}s<br>(%{percent})')

    st.plotly_chart(fig, use_container_width=True)

    # --- Table: Breakdown by MatchKey ---
    breakdown = (
        group.groupby(['MatchKey', 'RobotAction'])
        .size()
        .reset_index(name='Count')
    )
    breakdown['Seconds'] = breakdown['Count'] * SECONDS_PER_ROW
    breakdown = breakdown.sort_values(by=['MatchKey', 'RobotAction'])

    st.dataframe(breakdown, use_container_width=True, hide_index=True, column_config={"Count": None})

    # Buttons for match pages
    st.write("#### View Match Breakdown:")
    match_cols = st.columns(min(len(group['MatchKey'].unique()), 6))  # Up to 4 buttons per row
    for i, match_key in enumerate(sorted(group['MatchKey'].unique())):
        col = match_cols[i % len(match_cols)]
        with col:
            if st.button(f"🔍 {match_key}", key=f"{event}_{match_key}"):
                st.session_state['selected_match'] = (event, match_key)
                st.switch_page("match_breakdown.py")  # assumes the match breakdown page exists
    
    st.divider()

