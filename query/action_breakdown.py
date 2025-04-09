from string import Template

_QUERY = Template("""
-- CREATE MACRO IF NOT EXISTS LINE_TO_POINT(A, B, C, x, y) AS ABS(A*x + B*y + C) / SQRT(POWER(A, 2) + POWER(B,2));

SET VARIABLE START_BUTTON_MASK = 1 << 7;
SET VARIABLE BACK_BUTTON_MASK = 1 << 6;
SET VARIABLE LEFT_BUMPER_BUTTON_MASK = 1 << 4;
SET VARIABLE RIGHT_BUMPER_BUTTON_MASK = 1 << 5;
SET VARIABLE ALGAE_LOW = 0.5;
SET VARIABLE ALGAE_HIGH = 0.9;
                  
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
      ABS(OmegaVelocity) + ABS(XVelocity) + ABS(YVelocity) < 0.01 AS RobotStopped,
      LINE_TO_POINT(getvariable('Blue1A'), getvariable('Blue1B'), getvariable('Blue1C'), PoseX, PoseY) As DistanceToBlueLeftFeeder,
      LINE_TO_POINT(getvariable('Blue2A'), getvariable('Blue2B'), getvariable('Blue2C'), PoseX, PoseY) As DistanceToBlueRightFeeder,
      LINE_TO_POINT(getvariable('Red1A'), getvariable('Red1B'), getvariable('Red1C'), PoseX, PoseY) As DistanceToRedLeftFeeder,
      LINE_TO_POINT(getvariable('Red2A'), getvariable('Red2B'), getvariable('Red2C'), PoseX, PoseY) As DistanceToRedRightFeeder,
      LEAST(DistanceToBlueLeftFeeder, DistanceToBlueRightFeeder, DistanceToRedLeftFeeder, DistanceToRedRightFeeder) AS MinDistanceToFeeder,
      MinDistanceToFeeder < 1.5 AND IntakeState = 'INTAKING' AS IsFeeding,
      IntakeState LIKE '%INTAK%' OR IntakeState LIKE '%UNJAM%' AS IsIntaking,
      IntakeState = 'STOPPED' AND InnerSensorTriggered AS IntakeStoppedWithCoral,
      PoseX > 8.775 AS OnRedSideOfField,
      AllianceStation < 4 AS IsRedAlliance,
      CONCAT(loop_count, filename) AS UniqueKey,
      AlgaeIntakeCurrent > 1.0 AS AlgaeRollerRunning,
      LOWER(EventName) AS EventName,
      UPPER(CONCAT(MATCH_TYPE_STRING(MatchType), CAST(MatchNumber AS INTEGER))) AS MatchKey,
      RIGHT(CAST(to_seconds(CAST(MatchTime AS INTEGER)) AS VARCHAR),5) AS MatchTimeFormatted,
      (NOT IsRedAlliance AND OnRedSideOfField) OR (IsRedAlliance AND NOT OnRedSideOfField) AS OnOppositeSideOfField,
      NOT Autonomous AND Enabled AS TeleopEnabled,
      JoystickPOV = 90 AS JoystickScoreInProcessor,
      JoystickPOV = 270 AS JoystickScoreInNet,
      CAST(JoystickButtons AS LONG) AS JoystickButtonsCasted,
      JoystickAxis[3] > 0.1 OR JoystickAxis[4] > 0.1 AS L4Button,
      JoystickButtonsCasted & getvariable('START_BUTTON_MASK') OR JoystickButtonsCasted & getvariable('BACK_BUTTON_MASK') AS L3Button,
      JoystickButtonsCasted & getvariable('LEFT_BUMPER_BUTTON_MASK') OR JoystickButtonsCasted & getvariable('RIGHT_BUMPER_BUTTON_MASK') AS L2Button,
      JoystickPOV = 0 AS L1Button,
  
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
               last_value(Max("/DriverStation/Autonomous") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Autonomous,
               last_value(Max("/RealOutputs/Launcher/State") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS IntakeState,
               last_value(max("/DriverStation/MatchTime") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchTime,
               last_value(max("/Launcher/Sensor/InnerSensorTriggered") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS InnerSensorTriggered,
               last_value(max("/Launcher/Sensor/ReefSensorTriggered") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ReefSensorTriggered,
               last_value(max("/RealOutputs/Algae/HasAlgae") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS HasAlgae,
               last_value(max("/DriverStation/MatchNumber") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchNumber,
               last_value(max("/DriverStation/MatchType") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MatchType,
               last_value(max("/DriverStation/EventName") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EventName,
               last_value(max("/DriverStation/AllianceStation") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS AllianceStation,
               last_value(max("/RealOutputs/Climber/Setpoint") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ClimberSetpoint,
               last_value(max("/RealOutputs/Climber/degrees") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ClimberAngleDegrees,
               last_value(max("/DriverStation/Joystick0/ButtonValues") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JoystickButtons,
               last_value(max("/DriverStation/Joystick0/POVs")[1] IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JoystickPOV,
               last_value(max("/DriverStation/Joystick0/AxisValues") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS JoystickAxis,
               last_value(max("/DriverStation/EventName") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ElevatorHeight,
               last_value(max("/RealOutputs/Elevator/Profile/SetpointPosition") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ElevatorSetpoint,
               last_value(max("/RealOutputs/Algae/PivotDegrees") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS AlgaeGripperAngleDegrees,
               last_value(max("/Algae/RollerMotorCurrentAmps") IGNORE nulls) OVER (PARTITION BY filename ORDER BY loop_count ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS AlgaeIntakeCurrent,
       FROM frc2025
        GROUP BY loop_count, filename
        ORDER BY loop_count)
      WHERE TeleopEnabled $extra_where
),
action_table AS (
  SELECT EventName, MatchKey, MatchTimeFormatted AS MatchTime, loop_count, timestamp,
  MinDistanceToFeeder, DistanceToRedLeftFeeder, DistanceToRedRightFeeder,

  ClimberSetpoint > -1.047 AS AttemptingToClimb,
  ClimberSetpoint < -1.745 AND ClimberAngleDegrees > -100 AS ClimbInProgress,
  ClimberAngleDegrees < -100 AS Climbed,
  JoystickScoreInNet AS ScoringAlgaeInNet,
  JoystickScoreInProcessor AS ScoringAlgaeInProcessor,
  OnOppositeSideOfField AS DefendingOrStealingAlgae,
  L4Button AS AligningToL4,
  L3Button AS AligningToL3,
  L2Button AS AligningToL2,
  L1Button AS AligningToL1,
  IsFeeding AS IntakingCoral,
  AlgaeGripperAngleDegrees > 5 AND AlgaeRollerRunning AS IntakingAlgaeFromFloor,
  ElevatorSetpoint = getvariable('ALGAE_LOW') OR ElevatorSetpoint = getvariable('ALGAE_HIGH') AS GrabbingAlgaeFromReef,  
  
  FROM data_table
)
SELECT EventName, MatchKey, MatchTime, loop_count, timestamp,
  MinDistanceToFeeder, DistanceToRedLeftFeeder, DistanceToRedRightFeeder,
  CASE WHEN AttemptingToClimb THEN 'Attempting To Climb'
  WHEN ClimbInProgress THEN 'Climb in Progress'
  WHEN Climbed THEN 'Climbed'
  WHEN ScoringAlgaeInNet THEN 'Scoring Algae in the Net'
  WHEN ScoringAlgaeInProcessor THEN 'Scoring Algae in the Processor'
  WHEN DefendingOrStealingAlgae THEN 'Stealing Algae or Defending'
  WHEN AligningToL4 THEN 'Aligning to L4'
  WHEN AligningToL3 THEN 'Aligning to L3'
  WHEN AligningToL2 THEN 'Aligning to L2'
  WHEN AligningToL1 THEN 'Aligning to L1'
  -- WHEN HomeAfterScoring THEN 'Home after Scoring'
  WHEN IntakingCoral THEN 'Intaking Coral'
  WHEN IntakingAlgaeFromFloor THEN 'Intaking Algae from the Floor'
  WHEN GrabbingAlgaeFromReef THEN 'Grabbing Algae from the Reef'
  ELSE 'Driving' END AS RobotAction
  
FROM action_table
ORDER BY EventName, MatchKey, loop_count;
""")

def all():
    return _QUERY.substitute(extra_where="")

def by_event(event: str):
    return _QUERY.substitute(extra_where=f"AND EventName = {event.upper()}")

def by_match(event: str, match: str):
    return _QUERY.substitute(extra_where=f"AND EventName = '{event.upper()}' AND MatchKey = '{match.upper()}'")
