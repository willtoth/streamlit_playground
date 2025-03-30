from typing import List, Optional, Tuple

class SQLBuilder:
    def __init__(self, base_table: str):
        """
        Initialize the SQL builder with the base table name.
        
        Args:
            base_table: The name of the table containing the sparse data
        """
        self.base_table = base_table
        self.columns: List[Tuple[str, str]] = []
        self.required_columns = ['loop_count', 'filename']
        
    def add_column(self, column_path: str, alias: Optional[str] = None) -> 'SQLBuilder':
        """
        Add a column to be processed with the sparse-to-dense pattern.
        
        Args:
            column_path: The full path/name of the column in the source table
            alias: Optional alias for the column. If not provided, uses the column name
            
        Returns:
            self for method chaining
        """
        if alias is None:
            alias = column_path
        
        self.columns.append((column_path, alias))
        return self
    
    def add_columns(self, column_path: List[str]) -> 'SQLBuilder':
        [self.add_column(col) for col in column_path]
        return self
    
    def build(self) -> str:
        """
        Build the SQL query that converts sparse data to dense.
        
        Returns:
            The complete SQL query string
        """
        # Build the column list with the sparse-to-dense pattern
        column_definitions: List[str] = []
        
        # Add required columns first
        for col in self.required_columns:
            column_definitions.append(f"    {col}")
            
        # Add the processed columns
        for col_path, alias in self.columns:
            column_def = (
                f"    last_value(max(\"{col_path}\") IGNORE nulls) "
                f"OVER (PARTITION BY filename ORDER BY loop_count "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS '{alias}'"
            )
            column_definitions.append(column_def)
            
        # Build the complete query with proper formatting
        query = "SELECT\n"
        query += ",\n".join(column_definitions)
        query += f"\nFROM {self.base_table}\n"
        query += "GROUP BY loop_count, filename\n"
        query += "ORDER BY loop_count"
        
        return query

# Example usage:
if __name__ == "__main__":
    builder = SQLBuilder("txwac")
    
    # Add columns with their paths
    query = (builder
        .add_column("/RealOutputs/Launcher/State", "LauncherState")
        .add_column("/RealOutputs/Elevator/Profile/GoalPosition", "ElevatorGoalPosition")
        .add_column("/RealOutputs/Laterator/Setpoint", "LateratorSetpoint")
        .add_column("/RealOutputs/Launcher/AutoScoreEnabled", "AutoScoreArmed")
        .add_column("/Launcher/Sensor/ReefSensorTriggered", "ReefSensorTriggered")
        .add_column("/DriverStation/MatchTime", "MatchTime")
        .build())
    
    print(query)
