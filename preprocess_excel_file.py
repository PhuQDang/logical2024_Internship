import pandas as pd
import numpy as np
from pathlib import Path
from typing import Callable, List, Optional, Dict, Tuple

class AdvancedExcelProcessor:
    def __init__(self, file_path: str):
        """
        Initialize the Excel processor with a file path.
        
        Parameters:
        file_path (str): Path to the Excel file to process
        """
        self.file_path = file_path
        self.df = None
        self.original_df = None
        self.changes = {
            'total_cells': 0,
            'cells_modified': 0,
            'rows_affected': 0,
            'columns_affected': 0,
            'empty_columns_removed': 0,
            'whitespace_columns_removed': 0,
            'total_columns_removed': 0,
            'title_rows_removed': 0
        }
        
    def load_file(self, header=None) -> bool:
        """Load the Excel file into a DataFrame."""
        try:
            self.df = pd.read_excel(self.file_path, header=header)
            self.original_df = self.df.copy()
            self.changes['total_cells'] = self.df.size
            return True
        except Exception as e:
            print(f"Error loading file: {str(e)}")
            return False
    
    def remove_title_rows(self, title_indicators: Optional[List[str]] = None) -> bool:
        """Remove rows that appear to be titles or headers."""
        try:
            if self.df is None:
                self.load_file(header=None)
                
            if title_indicators is None:
                title_indicators = [
                    'title', 'header', 'section', 'total', 'subtotal',
                    'summary', 'heading', 'chapter'
                ]
            
            def is_title_row(row):
                row_str = ' '.join(str(val).lower() for val in row if pd.notna(val))
                
                if any(indicator in row_str.lower() for indicator in title_indicators):
                    return True
                    
                non_empty_cells = sum(pd.notna(val) for val in row)
                
                if non_empty_cells <= 2 and len(row) > 4:
                    return True
                    
                if row_str.isupper() and len(row_str) > 5:
                    return True
                    
                if non_empty_cells == 1 and len(row_str) < 50:
                    return True
                    
                return False
            
            keep_mask = ~self.df.apply(is_title_row, axis=1)
            title_rows_removed = sum(~keep_mask)
            
            self.df = self.df[keep_mask].reset_index(drop=True)
            self.changes['title_rows_removed'] = title_rows_removed
            
            print(f"Removed {title_rows_removed} title rows")
            return True
            
        except Exception as e:
            print(f"Error removing title rows: {str(e)}")
            return False
    
    def modify_column(self, column_name: str, modification_function: Callable) -> bool:
        """Modify values in a specified column using a custom function."""
        try:
            if self.df is None:
                self.load_file()
                
            if column_name not in self.df.columns:
                raise ValueError(f"Column '{column_name}' not found in the Excel file")
            
            original_values = self.df[column_name].copy()
            self.df[column_name] = self.df[column_name].apply(modification_function)
            
            # Update changes tracking
            modified_mask = original_values != self.df[column_name]
            self.changes['cells_modified'] += sum(modified_mask)
            self.changes['rows_affected'] = len(set(np.where(modified_mask)[0]))
            self.changes['columns_affected'] += 1
            
            return True
            
        except Exception as e:
            print(f"Error modifying column: {str(e)}")
            return False
    
    def clean_data(self) -> bool:
        """Clean up data alignment and remove empty columns."""
        try:
            if self.df is None:
                self.load_file()
            
            def is_misplaced(value, column_name):
                if pd.isna(value):
                    return False
                    
                str_value = str(value).strip()
                
                column_rules = {
                    'date': lambda x: pd.to_datetime(x, errors='coerce') is not pd.NaT,
                    'email': lambda x: '@' in str(x) and '.' in str(x),
                    'phone': lambda x: any(c.isdigit() for c in str(x)),
                    'number': lambda x: str(x).replace('.','').isdigit(),
                }
                
                rule = None
                for key in column_rules:
                    if type(key) != str:
                        if key in column_name:
                            rule = column_rules[key]
                            break
                    if key.lower() in column_name.lower():
                        rule = column_rules[key]
                        break
                
                if rule:
                    try:
                        return not rule(str_value)
                    except:
                        return True
                        
                return False
            
            # Process misplaced values
            for column in self.df.columns:
                misplaced_mask = self.df[column].apply(lambda x: is_misplaced(x, column))
                misplaced_values = self.df.loc[misplaced_mask, column].dropna()
                
                self.df.loc[misplaced_mask, column] = np.nan
                
                for value in misplaced_values:
                    for other_column in self.df.columns:
                        if other_column != column and not is_misplaced(value, other_column):
                            empty_slots = self.df[other_column].isna()
                            if empty_slots.any():
                                first_empty = empty_slots.idxmax()
                                self.df.loc[first_empty, other_column] = value
                                break
            
            # Remove empty columns
            empty_columns = self.df.columns[self.df.isna().all()]
            self.df = self.df.drop(columns=empty_columns)
            self.changes['empty_columns_removed'] = len(empty_columns)
            
            # Remove whitespace-only columns
            whitespace_columns = []
            for column in self.df.columns:
                if self.df[column].dtype == object:
                    if self.df[column].str.strip().str.len().fillna(0).eq(0).all():
                        whitespace_columns.append(column)
            
            self.df = self.df.drop(columns=whitespace_columns)
            self.changes['whitespace_columns_removed'] = len(whitespace_columns)
            self.changes['total_columns_removed'] = len(empty_columns) + len(whitespace_columns)
            
            # Update changes tracking
            cells_modified = (self.original_df != self.df).sum().sum()
            self.changes['cells_modified'] = cells_modified
            self.changes['rows_affected'] = len(set(np.where(self.original_df != self.df)[0]))
            self.changes['columns_affected'] = len(set(np.where(self.original_df != self.df)[1]))
            
            return True
            
        except Exception as e:
            print(f"Error cleaning data: {str(e)}")
            return False
    
    def save_file(self, output_path: Optional[str] = None) -> Tuple[str, Dict]:
        """
        Save the processed DataFrame and return the path and change summary.
        
        Returns:
        Tuple[str, Dict]: (output_path, changes_dictionary)
        """
        try:
            if self.df is None:
                raise ValueError("No data to save. Please load a file first.")
                
            if output_path is None:
                input_path = Path(self.file_path)
                output_path = input_path.parent / f"{input_path.stem}_processed{input_path.suffix}"
            
            self.df.to_excel(output_path, index=False)
            print(f"File saved successfully to {output_path}")
            return str(output_path), self.changes
            
        except Exception as e:
            print(f"Error saving file: {str(e)}")
            return None, None

    def print_summary(self):
        """Print a summary of all changes made to the file."""
        print("\nSummary of changes:")
        print(f"Total cells processed: {self.changes['total_cells']}")
        print(f"Cells modified: {self.changes['cells_modified']}")
        print(f"Rows affected: {self.changes['rows_affected']}")
        print(f"Columns affected: {self.changes['columns_affected']}")
        print(f"Title rows removed: {self.changes['title_rows_removed']}")
        print(f"Empty columns removed: {self.changes['empty_columns_removed']}")
        print(f"Whitespace-only columns removed: {self.changes['whitespace_columns_removed']}")
        print(f"Total columns removed: {self.changes['total_columns_removed']}")

# Example usage
if __name__ == "__main__":
    # Create processor instance
    fname = 'dsdn_1997_2024.xlsx'
    processor = AdvancedExcelProcessor(fname)
    
    # Perform all cleanup operations
    processor.remove_title_rows()
    processor.clean_data()
    
    # Modify specific columns if needed
    processor.modify_column("Value", lambda x: float(x) * 2 if isinstance(x, (int, float)) else x)
    
    # Save and get summary
    output_path, changes = processor.save_file()
    
    # # Print summary of all changes
    # processor.print_summary()