-- Drive Station Lines (Standard Form) Ax + By + C = 0
SET VARIABLE Blue1A = -1.22;
SET VARIABLE Blue1B = -1.723;
SET VARIABLE Blue1C = 2.086;

SET VARIABLE Blue2A = 1.25;
SET VARIABLE Blue2B = -1.723;
SET VARIABLE Blue2C = 11.751;

SET VARIABLE Red1A = 1.215;
SET VARIABLE Red1B = -1.718;
SET VARIABLE Red1C = -19.258;

SET VARIABLE Red2A = -1.258;
SET VARIABLE Red2B = -1.7;
SET VARIABLE Red2C = 33.659;

WITH data_table AS (
  SELECT *,
         NOT LAG(IsFeeding) OVER (PARTITION BY filename ORDER BY loop_count) AND IsFeeding AS IsFeedingRisingEdge,
         NOT LAG(IsIntaking) OVER (PARTITION BY filename ORDER BY loop_count) AND IsFeeding AS IsIntakingRisingEdge,
         NOT LAG(InnerSensorTriggered) OVER (PARTITION BY filename ORDER BY loop_count) AND InnerSensorTriggered AS InnerSensorTriggeredRisingEdge,
         NOT LAG(IntakeStoppedWithCoral) OVER (PARTITION BY filename ORDER BY loop_count) AND IntakeStoppedWithCoral AS IntakeStoppedWithCoralRisingEdge,
  FROM (
    SELECT *,
      ABS(OmegaVelocity) + ABS(XVelocity) + ABS(YVelocity) < 0.01 AS RobotStopped,
      LINE_TO_POINT(getvariable('Blue1A'), getvariable('Blue1B'), getvariable('Blue1C'), PoseX, PoseY) As DistanceToBlueLeftFeeder,
      LINE_TO_POINT(getvariable('Blue2A'), getvariable('Blue2B'), getvariable('Blue2C'), PoseX, PoseY) As DistanceToBlueRightFeeder,
      LINE_TO_POINT(getvariable('Red1A'), getvariable('Red1B'), getvariable('Red1C'), PoseX, PoseY) As DistanceToRedLeftFeeder,
      LINE_TO_POINT(getvariable('Red2A'), getvariable('Red2B'), getvariable('Red2C'), PoseX, PoseY) As DistanceToRedRightFeeder,
      LEAST(DistanceToBlueLeftFeeder, DistanceToBlueRightFeeder, DistanceToRedLeftFeeder, DistanceToRedRightFeeder) AS MinDistanceToFeeder,
      MinDistanceToFeeder < 1.5 AND IntakeState = 'INTAKING' AS IsFeeding,
      IntakeState LIKE '%INTAK%' AS IsIntaking,
      IntakeState = 'STOPPED' AND InnerSensorTriggered AS IntakeStoppedWithCoral,
      FROM (
        SELECT loop_count, filename,
               MIN(timestamp) as timestamp,
               last_value(Max("/RealOutputs/Odometry/Robot").x IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS PoseX,
               last_value(Max("/RealOutputs/Odometry/Robot").y IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS PoseY,
               last_value(Max("/RealOutputs/Odometry/Robot").value IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS PoseTheta,
               last_value(Max("/RealOutputs/SwerveChassisSpeeds/Measured").vx IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS XVelocity,
               last_value(Max("/RealOutputs/SwerveChassisSpeeds/Measured").vy IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS YVelocity,
               last_value(Max("/RealOutputs/SwerveChassisSpeeds/Measured").omega IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS OmegaVelocity,
               last_value(Max("/DriverStation/Enabled") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Enabled,
               last_value(Max("/RealOutputs/Launcher/State") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS IntakeState,
               last_value(max("/DriverStation/MatchTime") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchTime,
               last_value(max("/Launcher/Sensor/InnerSensorTriggered") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS InnerSensorTriggered,
               last_value(max("/DriverStation/MatchNumber") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchNumber,
               last_value(max("/DriverStation/MatchType") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchType,
               last_value(max("/DriverStation/EventName") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EventName, 
       FROM frc2025
        GROUP BY loop_count, filename
        ORDER BY loop_count)
      WHERE enabled)
),
intaking_sequence AS (
  SELECT *, 
         SUM(CASE 
             WHEN IsFeedingRisingEdge THEN 1 
             ELSE 0 
         END) OVER (PARTITION BY filename ORDER BY timestamp) AS intake_number
  FROM data_table
  ORDER BY filename, loop_count
),
event_alignment AS (
  SELECT filename, intake_number,
        MAX(MatchTime) AS MatchTime,
        MAX(EventName) AS EventName,
        MAX(MatchType) AS MatchType,
        MAX(MatchNumber) AS MatchNumber,
        MIN(timestamp) FILTER (WHERE IsFeedingRisingEdge) AS first_feed_rising_edge,
        MIN(timestamp) FILTER (WHERE InnerSensorTriggeredRisingEdge) AS first_reef_sensor_timestamp
  FROM intaking_sequence
  WHERE IsIntaking
  GROUP BY filename, intake_number
)
  SELECT LOWER(EventName) AS EventName, UPPER(CONCAT(MATCH_TYPE_STRING(MatchType), CAST(MatchNumber AS INTEGER))) AS Match, RIGHT(CAST(to_seconds(CAST(MatchTime AS INTEGER)) AS VARCHAR),5) AS MatchTime,
    first_reef_sensor_timestamp - first_feed_rising_edge AS IntakeTime
    FROM event_alignment
    WHERE IntakeTime IS NOT NULL AND IntakeTime < 50
    ORDER BY filename, intake_number;
