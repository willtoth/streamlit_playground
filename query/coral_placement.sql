WITH datatable AS (
  SELECT *,
         NOT LAG(ReefSensorTriggered) OVER (PARTITION BY datakey ORDER BY loop_count) AND ReefSensorTriggered AND LauncherState = 'SHOOTING' AS ReefSensorTriggeredRisingEdge,
         NOT LAG(LateratorExtended) OVER (PARTITION BY datakey ORDER BY loop_count) AND LateratorExtended AS LateratorExtendedRisingEdge
  FROM (
    SELECT loop_count, filename as datakey,
        last_value(max(timestamp) IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS timestamp,
        last_value(max("/RealOutputs/Launcher/State") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LauncherState,
        last_value(max("/RealOutputs/Elevator/Profile/GoalPosition") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ElevatorGoalPosition,
        last_value(max("/RealOutputs/Laterator/Setpoint") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LateratorSetpoint,
        last_value(max("/RealOutputs/Launcher/AutoScoreEnabled") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS AutoScoreArmed,
        last_value(max("/Launcher/Sensor/ReefSensorTriggered") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ReefSensorTriggered,
        last_value(max("/DriverStation/MatchTime") IGNORE nulls) OVER (PARTITION BY datakey ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchTime,
        last_value(max("/DriverStation/MatchNumber") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchNumber,
        last_value(max("/DriverStation/MatchType") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchType,
        last_value(max("/DriverStation/EventName") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EventName, 
        LateratorSetpoint > 0.25 AS LateratorExtended
    FROM frc2025
  GROUP BY loop_count, datakey
  ORDER BY loop_count)
),
shooting_sequence AS (
  SELECT *, 
         SUM(CASE 
             WHEN LateratorExtendedRisingEdge THEN 1 
             ELSE 0 
         END) OVER (PARTITION BY datakey ORDER BY timestamp) AS shot_number
  FROM datatable WHERE LateratorExtended
  ORDER BY datakey, loop_count
),
event_alignment AS (
  SELECT datakey, shot_number,
        MAX(MatchTime) AS MatchTime,
        MAX(EventName) AS EventName,
        MAX(MatchType) AS MatchType,
        MAX(MatchNumber) AS MatchNumber,
        MIN(timestamp) FILTER (WHERE LateratorExtendedRisingEdge) AS first_laterator_timestamp,
        MIN(timestamp) FILTER (WHERE ReefSensorTriggeredRisingEdge) AS first_reef_sensor_timestamp
  FROM shooting_sequence
  GROUP BY datakey, shot_number
)
SELECT 
  LOWER(EventName) AS EventName,
  UPPER(CONCAT(MATCH_TYPE_STRING(MatchType),
  CAST(MatchNumber AS INTEGER))) AS Match,
  RIGHT(CAST(to_seconds(CAST(MatchTime AS INTEGER)) AS VARCHAR),5) AS MatchTime,
       first_reef_sensor_timestamp - first_laterator_timestamp AS shot_duration
FROM event_alignment
WHERE shot_duration IS NOT NULL
ORDER BY datakey, shot_number;
