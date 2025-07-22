import pandas as pd
import os
from collections import defaultdict
import numpy as np
from typing import Dict, List, Tuple
import hashlib

class CSVSchemaAnalyzer:
    def __init__(self, directory_path: str):
        self.directory = directory_path
        self.file_metadata = {}
        self.column_stats = defaultdict(dict)
        self.relationships = []
        self.suggested_primary_keys = {}
        self.suggested_foreign_keys = []
    
    def analyze_all_files(self):
        """Process all CSV files in the directory"""
        for filename in os.listdir(self.directory):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.directory, filename)
                self.analyze_file(filepath)
        
        self.find_relationships()
        self.suggest_keys()
    
    def analyze_file(self, filepath: str):
        """Analyze a single CSV file"""
        try:
            # Read first 1000 rows for analysis (adjust as needed)
            df = pd.read_csv(filepath, nrows=1000)
            
            # Generate file metadata
            file_hash = self._file_hash(filepath)
            self.file_metadata[filepath] = {
                'filename': os.path.basename(filepath),
                'columns': list(df.columns),
                'row_count': len(df),
                'sample_data': df.head(1).to_dict(orient='records')[0],
                'file_hash': file_hash
            }
            
            # Analyze each column
            for column in df.columns:
                self.analyze_column(filepath, column, df[column])
                
        except Exception as e:
            print(f"Error processing {filepath}: {str(e)}")
    
    def analyze_column(self, filepath: str, column: str, series: pd.Series):
        """Perform column-level analysis"""
        # Basic stats
        unique_count = series.nunique()
        null_count = series.isnull().sum()
        dtype = str(series.dtype)
        
        # Value length analysis (for string columns)
        if dtype == 'object':
            lengths = series.dropna().astype(str).apply(len)
            len_stats = {
                'min_len': int(lengths.min()),
                'max_len': int(lengths.max()),
                'avg_len': float(lengths.mean())
            }
        else:
            len_stats = {}
        
        # Store column statistics
        self.column_stats[filepath][column] = {
            'dtype': dtype,
            'unique_count': int(unique_count),
            'null_count': int(null_count),
            'unique_ratio': float(unique_count / len(series)),
            'sample_values': list(series.dropna().unique()[:5]),
            **len_stats
        }
    
    def find_relationships(self):
        """Find potential relationships between tables"""
        # Group columns by their characteristics
        column_groups = defaultdict(list)
        
        for filepath, columns in self.column_stats.items():
            for col_name, stats in columns.items():
                key = (stats['dtype'], stats['unique_ratio'])
                column_groups[key].append((filepath, col_name))
        
        # Look for potential foreign key relationships
        for group, columns in column_groups.items():
            if len(columns) > 1 and group[0] != 'object':  # Skip high-cardinality text fields
                # Sort by uniqueness (more unique = more likely to be PK)
                sorted_cols = sorted(columns, 
                                  key=lambda x: self.column_stats[x[0]][x[1]]['unique_ratio'], 
                                  reverse=True)
                
                # The most unique column is likely the PK
                pk_candidate = sorted_cols[0]
                
                # Others may be FKs referencing it
                for other in sorted_cols[1:]:
                    if self._values_compatible(pk_candidate, other):
                        self.relationships.append({
                            'pk_file': pk_candidate[0],
                            'pk_column': pk_candidate[1],
                            'fk_file': other[0],
                            'fk_column': other[1],
                            'confidence': self._calculate_confidence(pk_candidate, other)
                        })
    
    def suggest_keys(self):
        """Suggest primary and foreign keys based on analysis"""
        # Suggest primary keys (columns with high uniqueness)
        for filepath, columns in self.column_stats.items():
            best_pk = None
            best_score = 0
            
            for col_name, stats in columns.items():
                # Score based on uniqueness and null count
                score = stats['unique_ratio'] * (1 - (stats['null_count'] / self.file_metadata[filepath]['row_count']))
                
                # Penalize string columns unless they look like IDs
                if stats['dtype'] == 'object':
                    if not (stats['min_len'] == stats['max_len'] and stats['avg_len'] < 20):
                        score *= 0.5
                
                if score > best_score:
                    best_score = score
                    best_pk = col_name
            
            if best_pk and best_score > 0.8:  # Threshold for PK confidence
                self.suggested_primary_keys[filepath] = best_pk
        
        # Suggest foreign keys from relationships
        for rel in self.relationships:
            if rel['pk_file'] in self.suggested_primary_keys:
                if rel['pk_column'] == self.suggested_primary_keys[rel['pk_file']]:
                    self.suggested_foreign_keys.append({
                        'source_file': rel['fk_file'],
                        'source_column': rel['fk_column'],
                        'target_file': rel['pk_file'],
                        'target_column': rel['pk_column'],
                        'confidence': rel['confidence']
                    })
    
    def _values_compatible(self, pk_candidate: Tuple[str, str], fk_candidate: Tuple[str, str]) -> bool:
        """Check if values in two columns are compatible"""
        pk_file, pk_col = pk_candidate
        fk_file, fk_col = fk_candidate
        
        pk_stats = self.column_stats[pk_file][pk_col]
        fk_stats = self.column_stats[fk_file][fk_col]
        
        # Basic type check
        if pk_stats['dtype'] != fk_stats['dtype']:
            return False
        
        # If PK has more unique values than FK, it's a possible relationship
        if pk_stats['unique_ratio'] < fk_stats['unique_ratio']:
            return False
        
        # Sample values check (at least some overlap)
        pk_samples = set(str(x) for x in pk_stats['sample_values'])
        fk_samples = set(str(x) for x in fk_stats['sample_values'])
        
        return len(pk_samples & fk_samples) > 0
    
    def _calculate_confidence(self, pk_candidate: Tuple[str, str], fk_candidate: Tuple[str, str]) -> float:
        """Calculate confidence score for a relationship"""
        pk_file, pk_col = pk_candidate
        fk_file, fk_col = fk_candidate
        
        pk_stats = self.column_stats[pk_file][pk_col]
        fk_stats = self.column_stats[fk_file][fk_col]
        
        # Base confidence on uniqueness ratios
        base_score = min(pk_stats['unique_ratio'], 1 - fk_stats['unique_ratio'])
        
        # Boost if sample values match exactly
        pk_samples = set(str(x) for x in pk_stats['sample_values'])
        fk_samples = set(str(x) for x in fk_stats['sample_values'])
        overlap = len(pk_samples & fk_samples)
        sample_boost = overlap / len(pk_samples) if len(pk_samples) > 0 else 0
        
        return min(1.0, base_score + (sample_boost * 0.3))
    
    def _file_hash(self, filepath: str) -> str:
        """Calculate file hash to identify duplicate files"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()
    
    def generate_report(self) -> Dict:
        """Generate a comprehensive report of findings"""
        return {
            'file_metadata': self.file_metadata,
            'column_statistics': self.column_stats,
            'suggested_primary_keys': self.suggested_primary_keys,
            'suggested_foreign_keys': self.suggested_foreign_keys,
            'all_relationships': self.relationships
        }
