import streamlit as st
from query.column_selector import ColumnSelector
from st_ant_tree import st_ant_tree


st.markdown("# Page 2 ❄️")
st.subheader("A simple and elegant checkbox tree for Streamlit.")

# Define tree data
tree_data = [
    {
        "value": "parent_1",
        "title": "Parent 1",
        "children": [
            {"value": "child_1", "title": "Child 1"},
            {"value": "child_2", "title": "Child 2"},
        ],
    },
    {"value": "parent_2", "title": "Parent 2"},
]

# Use the component
selected_values = st_ant_tree(
    treeData=tree_data,
    treeCheckable=True,
    allowClear=True
)

st.write(f"Selected values: {selected_values}")


st.sidebar.markdown("# Page 2 ❄️")

con = st.session_state['conn']

selector = ColumnSelector(con)
selector.render()

@st.cache_data
def getData():
  return con.execute("""

  WITH datatable AS (
    SELECT *,
          NOT LAG(ReefSensorTriggered) OVER (PARTITION BY filename ORDER BY loop_count) AND ReefSensorTriggered AND LauncherState = 'SHOOTING' AS ReefSensorTriggeredRisingEdge,
          NOT LAG(LateratorExtended) OVER (PARTITION BY filename ORDER BY loop_count) AND LateratorExtended AS LateratorExtendedRisingEdge
    FROM (
      SELECT loop_count, filename,
          last_value(max(timestamp) IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS timestamp,
          last_value(max("/RealOutputs/Launcher/State") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LauncherState,
          last_value(max("/RealOutputs/Elevator/Profile/GoalPosition") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ElevatorGoalPosition,
          last_value(max("/RealOutputs/Laterator/Setpoint") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LateratorSetpoint,
          last_value(max("/RealOutputs/Launcher/AutoScoreEnabled") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS AutoScoreArmed,
          last_value(max("/Launcher/Sensor/ReefSensorTriggered") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ReefSensorTriggered,
          last_value(max("/DriverStation/MatchTime") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchTime,
          LateratorSetpoint > 0.25 AS LateratorExtended
      FROM txwac
    GROUP BY loop_count, filename
    ORDER BY loop_count)
  ),
  shooting_sequence AS (
    SELECT *, 
          SUM(CASE 
              WHEN LateratorExtendedRisingEdge THEN 1 
              ELSE 0 
          END) OVER (PARTITION BY filename ORDER BY timestamp) AS shot_number
    FROM datatable WHERE LateratorExtended
    ORDER BY filename, loop_count
  ),
  event_alignment AS (
    SELECT filename, shot_number,
          MAX(MatchTime) AS MatchTime,
          MIN(timestamp) FILTER (WHERE LateratorExtendedRisingEdge) AS first_laterator_timestamp,
          MIN(timestamp) FILTER (WHERE ReefSensorTriggeredRisingEdge) AS first_reef_sensor_timestamp
    FROM shooting_sequence
    GROUP BY filename, shot_number
  )
  SELECT filename, shot_number, RIGHT(CAST(to_seconds(CAST(MatchTime AS INTEGER)) AS VARCHAR),5) AS MatchTime, first_laterator_timestamp, first_reef_sensor_timestamp, 
        first_reef_sensor_timestamp - first_laterator_timestamp AS shot_duration
  FROM event_alignment
  WHERE shot_duration IS NOT NULL
  ORDER BY filename, shot_number;


  """).fetchdf()

df = getData()

st.dataframe(df)
