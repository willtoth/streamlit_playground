import pandas as pd
import io
from datetime import timedelta

def df_to_vtt_stream(df: pd.DataFrame, teleop_offset: int) -> io.BytesIO:
    """
    Converts a DataFrame into a .vtt subtitle file and returns a BytesIO stream.

    Parameters:
    - df: pandas DataFrame with columns ['Event', 'Match', 'Matchtime', 'timestamp', 'RobotAction'], the timestamp
      row should start at the beginning of teleop.

    Returns:
    - io.BytesIO object containing the VTT content
    """

    initial_timestamp = df['timestamp'].min()

    def format_timestamp(seconds):
        seconds = (seconds - initial_timestamp) + teleop_offset
        td = timedelta(seconds=float(seconds))
        # Format: HH:MM:SS.mmm (WebVTT uses . for milliseconds, not , like SRT)
        return str(td)[:-3].rjust(12, '0')
    
    # Loop through rows and group by consecutive robot actions
    previous_action = None
    start_time = None
    start_match_time = None
    metadata = f"{df.iloc[0]['EventName']} {df.iloc[0]['MatchKey']}"

    lines = ["WEBVTT\n\n"]

    for _, row in df.iterrows():
        # Only process if the robot action changes
        if row['RobotAction'] != previous_action:
            if previous_action is not None:  # Skip for the first action
                end_time = format_timestamp(row['timestamp'])
                end_match_time = row['MatchTime']
                caption = f"{previous_action}"
                lines.append(f"{start_time} --> {end_time}\nTeleop {start_match_time} - {end_match_time} {caption}\n\n")
            
            # Update for the new action
            start_time = format_timestamp(row['timestamp'])
            start_match_time = row['MatchTime']
            previous_action = row['RobotAction']

    # Add the last group if there's any remaining action
    if previous_action is not None:
        end_time = format_timestamp(df.iloc[-1]['timestamp'])
        caption = f"{previous_action}"
        lines.append(f"{start_time} --> {end_time}\nTeleop {start_match_time} - 00:00 {caption}\n\n")

    # Convert to bytes and return a stream
    vtt_bytes = "\n".join(lines).encode("utf-8")
    return io.BytesIO(vtt_bytes)

if __name__ == "__main__":
    # Example DataFrame for testing
    data = {
        'Event': ['TXCMP2', 'TXCMP2', 'TXCMP2', 'TXCMP2', 'TXCMP2'],
        'Match': ['E14', 'E14', 'E14', 'E14', 'E14'],
        'Matchtime': ['02:15', '02:15', '02:15', '02:15', '02:15'],
        'timestamp': [418.1231, 418.1235, 418.2123, 418.2233, 418.2333],
        'RobotAction': ['Driving', 'Driving', 'Idle', 'Driving', 'Idle']
    }
    df = pd.DataFrame(data)

    # Generate VTT stream from the DataFrame
    vtt_stream = df_to_vtt_stream(df, 10)

    # Print the VTT content (for testing purposes, printing the first 200 bytes)
    print("\nVTT Content Preview:")
    print(vtt_stream.getvalue()[:200].decode("utf-8"))  # Print first 200 characters