import streamlit as st
from collections import defaultdict
import duckdb
from fuzzywuzzy import fuzz
from typing import List, Tuple, Optional, Dict
from st_ant_tree import st_ant_tree


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
    def filter_columns(columns: List[str], search_term: str) -> List[str]:
        """
        Filter columns using fuzzy search.
        
        Args:
            columns: List of column names
            search_term: Search term to filter by
            
        Returns:
            List of matching column names
        """
        if not search_term:
            return columns
            
        results = []
        for col in columns:
            score = fuzz.ratio(search_term.lower(), col.lower())
            if score > 60:  # Only include matches with score > 60%
                results.append(col)
        
        return results

    def build_tree_data(self, paths: List[str]) -> List[Dict]:
        """
        Build a tree structure from column names.
        
        Args:
            columns: List of column names
            
        Returns:
            Dictionary representing the tree structure
        """
        def insert_node(tree, parts, all_parts, full_path: str):
            if not parts:
                return
            node = next((item for item in tree if item["title"] == parts[0]), None)
            current_path = "/".join(all_parts[: len(all_parts) - len(parts) + 1])

            if full_path.startswith("/"):
                current_path = "/" + current_path
            
            if not node:
                node = {"value": current_path, "title": parts[0]}
                if len(parts) > 1:
                    node["children"] = []
                tree.append(node)
            if len(parts) > 1:
                insert_node(node.setdefault("children", []), parts[1:], all_parts, full_path)
        
        tree: List[Dict] = []
        for path in paths:
            parts = path.strip("/").split("/")
            insert_node(tree, parts, parts, path)

        return tree
        
    def get_selection(self, table_name):
        """Render the Streamlit interface."""
        st.title("Column Chooser")
        
        # Get available columns
        self.columns = self.get_available_columns(table_name)
        
        # Build and display the tree
        tree_data = self.build_tree_data(self.columns)
        return st_ant_tree(
            treeData=tree_data,
            showSearch=True,
            placeholder="Search and select",
            treeCheckable=True
        )
