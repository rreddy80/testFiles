import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
import os
import itertools

class ColumnRelationshipFinder:
    def __init__(self, directory_path):
        self.directory = directory_path
        self.column_profiles = defaultdict(dict)
        self.relationships = []
    
    def analyze_all_files(self):
        """Process all CSV files in directory"""
        csv_files = [f for f in os.listdir(self.directory) if f.endswith('.csv')]
        for file in csv_files:
            self.analyze_file(os.path.join(self.directory, file))
        self.find_all_relationships()
    
    def analyze_file(self, filepath):
        """Analyze a single CSV file"""
        try:
            df = pd.read_csv(filepath, nrows=1000)  # Sample data
            filename = os.path.basename(filepath)
            
            for col in df.columns:
                self.column_profiles[filename][col] = {
                    'dtype': str(df[col].dtype),
                    'sample_values': list(df[col].dropna().unique()[:20]),
                    'uniqueness': df[col].nunique() / len(df[col])
                }
        except Exception as e:
            print(f"Error processing {filepath}: {e}")

    def find_all_relationships(self, min_name_similarity=60, min_value_overlap=0.1):
        """
        Compare every column with every other column across all tables
        """
        # Get all (filename, column_name) pairs
        all_columns = []
        for file, cols in self.column_profiles.items():
            for col in cols:
                all_columns.append((file, col))
        
        # Compare all unique pairs
        for (file1, col1), (file2, col2) in itertools.combinations(all_columns, 2):
            if file1 == file2:  # Skip same table comparisons
                continue
            
            profile1 = self.column_profiles[file1][col1]
            profile2 = self.column_profiles[file2][col2]
            
            # Check basic compatibility
            if profile1['dtype'] != profile2['dtype']:
                continue
            
            # Calculate name similarity
            name_sim = fuzz.token_sort_ratio(col1.lower(), col2.lower())
            
            # Calculate value overlap
            set1 = set(str(x) for x in profile1['sample_values'])
            set2 = set(str(x) for x in profile2['sample_values'])
            overlap = len(set1 & set2)
            overlap_ratio = overlap / min(len(set1), len(set2)) if min(len(set1), len(set2)) > 0 else 0
            
            # Check if meets thresholds
            if name_sim >= min_name_similarity or overlap_ratio >= min_value_overlap:
                confidence = self.calculate_confidence(name_sim, overlap_ratio)
                
                self.relationships.append({
                    'table1': file1,
                    'column1': col1,
                    'table2': file2,
                    'column2': col2,
                    'confidence': confidence,
                    'name_similarity': name_sim,
                    'value_overlap': overlap_ratio,
                    'dtype': profile1['dtype']
                })
        
        # Sort by confidence
        self.relationships.sort(key=lambda x: x['confidence'], reverse=True)
    
    def calculate_confidence(self, name_sim, value_overlap):
        """Simple confidence calculation"""
        return (name_sim * 0.4 + value_overlap * 100 * 0.6) / 100
    
    def get_relationships_for_column(self, table, column):
        """Get all relationships for a specific column"""
        return [r for r in self.relationships 
                if (r['table1'] == table and r['column1'] == column) or
                (r['table2'] == table and r['column2'] == column)]
    
    def visualize_relationships(self, min_confidence=0.5):
        """Generate a network visualization"""
        import networkx as nx
        import matplotlib.pyplot as plt
        
        G = nx.Graph()
        
        # Add nodes (columns with table info)
        added_nodes = set()
        for rel in self.relationships:
            if rel['confidence'] >= min_confidence:
                node1 = f"{rel['table1']}.{rel['column1']}"
                node2 = f"{rel['table2']}.{rel['column2']}"
                if node1 not in added_nodes:
                    G.add_node(node1)
                    added_nodes.add(node1)
                if node2 not in added_nodes:
                    G.add_node(node2)
                    added_nodes.add(node2)
                G.add_edge(node1, node2, weight=rel['confidence'])
        
        # Draw the graph
        plt.figure(figsize=(15, 15))
        pos = nx.spring_layout(G, k=0.5, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_size=1000, node_color='lightblue')
        
        # Draw edges with weights
        edges = G.edges(data=True)
        nx.draw_networkx_edges(
            G, pos, 
            edgelist=[(u, v) for u, v, d in edges],
            width=[d['weight']*3 for u, v, d in edges],
            alpha=0.5
        )
        
        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=8)
        
        plt.title(f"Column Relationships (confidence ≥ {min_confidence})")
        plt.axis('off')
        plt.show()
