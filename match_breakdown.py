import streamlit as st
from query import action_breakdown
import plotly.express as px
from sql_formatter.core import format_sql

from datasource import tba, youtube
from transforms import subtitles

st.markdown("# Match Breakdown ⌛")

try:
    event_match = st.session_state['selected_match']
except:
    st.text("Select a match on the Robot Actions tab")
    st.stop()

event = event_match[0]
match = event_match[1]

st.sidebar.markdown(f"""# Event {event_match[0]} - Match {event_match[1]}

""")

con = st.session_state['conn']
robot_actions = st.session_state['robot_action_table']
df = robot_actions[(robot_actions['MatchKey'] == match) & (robot_actions['EventName'] == event)]

# Show pie chart breakdown for match
  
# Count actions
SECONDS_PER_ROW = 0.02
action_counts = df['RobotAction'].value_counts().reset_index()
action_counts.columns = ['RobotAction', 'Count']
action_counts['Seconds'] = action_counts['Count'] * SECONDS_PER_ROW

color_map = st.session_state['robot_action_colors']

# Create pie chart
fig = px.pie(action_counts,
                names='RobotAction',
                values='Seconds',
                title=f'Action Distribution for {event} Match {match}',
                color='RobotAction',
                color_discrete_map=color_map
)

    
fig.update_traces(textinfo='label+percent', hovertemplate='%{label}: %{value:.2f}s<br>(%{percent})')

st.plotly_chart(fig, use_container_width=True)

matchkey = tba.get_match_key(2025, event, match)
tba_link = tba.get_tba_url(matchkey)
statbotics_link = f"https://www.statbotics.io/match/{matchkey}"
url = tba.get_match_video(matchkey)


start_time = st.number_input("Teleop Start Time (s)", value=0, step=1)
subs = subtitles.df_to_vtt_stream(df, start_time)

if url:
    file_path = youtube.download_video(url)

    st.video(file_path, start_time=start_time, subtitles=subs)
else:
    st.text("⚠️ No video available for this match")


st.markdown(
    f"""
    <a href="{tba_link}" target="_blank">
        <img src="app/static/tba_logo.png" alt="The Blue Alliance" style="width:30px;height:auto;">
    </a>
    <a href="{url}" target="_blank">
        <img src="app/static/youtube.png" alt="Youtube" style="width:30px;height:auto;">
    </a>
    <a href="{statbotics_link}" target="_blank">
        <img src="app/static/statbotics.png" alt="Statbotics" style="width:30px;height:auto;">
    </a>
    """,
    unsafe_allow_html=True
)


st.divider()

# Show dataframe and SQL query at the bottom
st.dataframe(df)

# show_sql = st.checkbox("Show SQL")

# if show_sql:
#     st.code(sql, language="sql")
