import streamlit as st
import duckdb
from fuzzywuzzy import fuzz
from typing import List, Tuple, Optional

class ColumnSelector:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize the column selector.
        
        Args:
            conn: DuckDB connection to use for queries
        """
        self.conn = conn
        self.columns: List[str] = []
        self.selected_columns: List[str] = []
            
    def get_available_columns(self, table_name: str) -> List[str]:
        """
        Get all available columns from the table using DuckDB.
        
        Args:
            table_name: Name of the table to get columns from
            
        Returns:
            List of column names
        """
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY column_name
        """
        
        try:
            result = self.conn.execute(query).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            st.error(f"Error getting columns: {str(e)}")
            return []
            
    @staticmethod
    def filter_columns(columns: List[str], search_term: str) -> List[Tuple[str, int]]:
        """
        Filter columns using fuzzy search.
        
        Args:
            columns: List of column names
            search_term: Search term to filter by
            
        Returns:
            List of tuples containing (column_name, match_score)
        """
        if not search_term:
            return [(col, 100) for col in columns]
            
        results = []
        for col in columns:
            score = fuzz.ratio(search_term.lower(), col.lower())
            if score > 60:  # Only include matches with score > 60%
                results.append((col, score))
        
        # Sort by score descending
        return sorted(results, key=lambda x: x[1], reverse=True)

        
    def render(self):
        """Render the Streamlit interface."""
        st.title("SQL Query Builder")
                    
        # Table selection
        table_name = st.text_input("Table Name", "txwac")
        
        # Get available columns
        self.columns = self.get_available_columns(table_name)
        
        # Search box
        search_term = st.text_input("Search columns", "")
        
        # Filter columns based on search
        filtered_columns = self.filter_columns(self.columns, search_term)
        
        # Display columns in a scrollable container
        st.subheader("Available Columns")
        
        # Create a container for the columns
        col_container = st.container()
        
        with col_container:
            # Display columns in a grid
            cols = st.columns(3)
            for i, (col, score) in enumerate(filtered_columns):
                col_idx = i % 3
                with cols[col_idx]:
                    if st.checkbox(f"{col} ({score}%)", key=f"col_{col}"):
                        if col not in self.selected_columns:
                            self.selected_columns.append(col)
                        elif not st.session_state.get(f"col_{col}", False):
                            self.selected_columns.remove(col)
        
        # Build query button
        if st.button("Build Query"):
            query = self.build_query(table_name)
            if query:
                # Display the query
                st.subheader("Generated Query")
                st.code(query, language="sql")
                
                # Add a copy button
                if st.button("Copy Query"):
                    st.write("Query copied to clipboard!")
            else:
                st.warning("Please select at least one column")

def main():
    # Example usage with connection management
    db_path = st.sidebar.text_input("Database Path", ":memory:")
    read_only = st.sidebar.checkbox("Read Only", value=True)
    
    try:
        conn = duckdb.connect(db_path, read_only=read_only)
        selector = ColumnSelector(conn)
        selector.render()
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main() 