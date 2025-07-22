import os
import json
import hashlib
import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
import itertools

class PersistentRelationshipAnalyzer:
    def __init__(self, directory_path, report_file='relationship_report.json'):
        self.directory = directory_path
        self.report_file = report_file
        self.column_profiles = defaultdict(dict)
        self.relationships = []
        self.file_hashes = {}
    
    def _calculate_file_hash(self, filepath):
        """Calculate MD5 hash of a file"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _get_all_file_hashes(self):
        """Calculate hashes for all CSV files"""
        hashes = {}
        for fname in os.listdir(self.directory):
            if fname.endswith('.csv'):
                full_path = os.path.join(self.directory, fname)
                hashes[fname] = self._calculate_file_hash(full_path)
        return hashes
    
    def _has_files_changed(self, cached_hashes):
        """Check if any files have changed since last analysis"""
        current_hashes = self._get_all_file_hashes()
        
        # If number of files changed
        if set(cached_hashes.keys()) != set(current_hashes.keys()):
            return True
        
        # If any file contents changed
        for fname, fhash in cached_hashes.items():
            if current_hashes.get(fname) != fhash:
                return True
        
        return False
    
    def load_or_analyze(self):
        """Load cached report or perform fresh analysis"""
        if os.path.exists(self.report_file):
            with open(self.report_file, 'r') as f:
                report = json.load(f)
            
            # Check if files have changed since last analysis
            if not self._has_files_changed(report['file_hashes']):
                print("Loading cached report (files unchanged)")
                self.relationships = report['relationships']
                self.column_profiles = report['column_profiles']
                self.file_hashes = report['file_hashes']
                return
        
        print("Performing fresh analysis...")
        self.analyze_all_files()
        self.save_report()
    
    def analyze_file(self, filepath):
        """Analyze a single CSV file"""
        try:
            df = pd.read_csv(filepath, nrows=1000)  # Sample data
            filename = os.path.basename(filepath)
            
            for col in df.columns:
                self.column_profiles[filename][col] = {
                    'dtype': str(df[col].dtype),
                    'sample_values': list(df[col].dropna().unique()[:20]),
                    'uniqueness': df[col].nunique() / len(df[col]),
                    'null_count': df[col].isna().sum()
                }
        except Exception as e:
            print(f"Error processing {filepath}: {e}")

    def analyze_all_files(self):
        """Process all CSV files in directory"""
        self.file_hashes = self._get_all_file_hashes()
        
        for fname, fhash in self.file_hashes.items():
            self.analyze_file(os.path.join(self.directory, fname))
        
        self.find_all_relationships()
    
    def find_all_relationships(self, min_name_similarity=60, min_value_overlap=0.1):
        """Compare every column with every other column across all tables"""
        all_columns = []
        for file, cols in self.column_profiles.items():
            for col in cols:
                all_columns.append((file, col))
        
        for (file1, col1), (file2, col2) in itertools.combinations(all_columns, 2):
            if file1 == file2:
                continue
            
            profile1 = self.column_profiles[file1][col1]
            profile2 = self.column_profiles[file2][col2]
            
            if profile1['dtype'] != profile2['dtype']:
                continue
            
            name_sim = fuzz.token_sort_ratio(col1.lower(), col2.lower())
            
            set1 = set(str(x) for x in profile1['sample_values'])
            set2 = set(str(x) for x in profile2['sample_values'])
            overlap = len(set1 & set2)
            min_samples = min(len(set1), len(set2))
            overlap_ratio = overlap / min_samples if min_samples > 0 else 0
            
            if name_sim >= min_name_similarity or overlap_ratio >= min_value_overlap:
                confidence = (name_sim * 0.4 + overlap_ratio * 100 * 0.6) / 100
                
                self.relationships.append({
                    'table1': file1,
                    'column1': col1,
                    'table2': file2,
                    'column2': col2,
                    'confidence': confidence,
                    'name_similarity': name_sim,
                    'value_overlap': overlap_ratio,
                    'dtype': profile1['dtype'],
                    'table1_uniqueness': profile1['uniqueness'],
                    'table2_uniqueness': profile2['uniqueness']
                })
        
        self.relationships.sort(key=lambda x: x['confidence'], reverse=True)
    
    def save_report(self):
        """Save analysis results to file"""
        report = {
            'file_hashes': self.file_hashes,
            'column_profiles': self.column_profiles,
            'relationships': self.relationships,
            'generated_at': pd.Timestamp.now().isoformat()
        }
        
        with open(self.report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Report saved to {self.report_file}")
    
    def get_strong_relationships(self, min_confidence=0.7):
        """Get relationships with confidence above threshold"""
        return [r for r in self.relationships if r['confidence'] >= min_confidence]
    
    def get_related_columns(self, table, column):
        """Find all columns related to a specific column"""
        related = []
        for rel in self.relationships:
            if rel['table1'] == table and rel['column1'] == column:
                related.append((rel['table2'], rel['column2'], rel['confidence']))
            elif rel['table2'] == table and rel['column2'] == column:
                related.append((rel['table1'], rel['column1'], rel['confidence']))
        return sorted(related, key=lambda x: x[2], reverse=True)
