#!/usr/bin/env python3
"""
RBNZ CBP Data Mapping Script
This script processes Excel files from RBNZ Monetary Policy Statements and creates
properly formatted CBP data and metadata files according to the runbook specifications.

Key features:
- Advanced description extraction following 5 structure patterns from runbook
- Generates CBP.NZL codes with proper format
- Creates two-column format with CODE and DESCRIPTION headers
- Handles quarterly date formatting (YYYY-QN)
- Generates comprehensive metadata with all required fields
"""

import pandas as pd
import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import zipfile

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RBNZMapper:
    def __init__(self, downloads_dir: str = "./downloads", output_dir: str = "./mapped_output"):
        self.downloads_dir = os.path.abspath(downloads_dir)
        self.output_dir = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Standard metadata fields according to runbook
        self.standard_metadata = {
            'FREQUENCY': 'Q',
            'AGGREGATION_TYPE': 'UNDEFINED',
            'ANNUALIZED': 'FALSE',
            'STATE': 'ACTIVE',
            'PROVIDER': 'AfricaAI',
            'SOURCE': 'RBNZ',
            'SOURCE_DESCRIPTION': 'Reserve Bank of New Zealand',
            'COUNTRY': 'NZL',
            'DATASET': 'CBP'
        }
        
        logger.info("RBNZ Mapper initialized.")

    def find_excel_files(self) -> List[str]:
        """
        Find all Excel files in the downloads directory
        """
        excel_files = []
        
        if not os.path.exists(self.downloads_dir):
            logger.error(f"Downloads directory does not exist: {self.downloads_dir}")
            return excel_files
        
        # Look for Excel files (.xlsx, .xls)
        for filename in os.listdir(self.downloads_dir):
            if filename.lower().endswith(('.xlsx', '.xls')) and not filename.startswith('~'):
                file_path = os.path.join(self.downloads_dir, filename)
                if os.path.isfile(file_path):
                    excel_files.append(file_path)
        
        logger.info(f"Found {len(excel_files)} Excel files in {self.downloads_dir}")
        for file_path in excel_files:
            logger.info(f"  - {os.path.basename(file_path)}")
        
        return excel_files

    def select_file_to_process(self, excel_files: List[str]) -> str:
        """
        Select which Excel file to process. Prioritizes RBNZ/MPS files.
        """
        if not excel_files:
            return None
        
        if len(excel_files) == 1:
            return excel_files[0]
        
        # Prioritize files that look like RBNZ/MPS data
        priority_patterns = ['mps', 'monetary', 'policy', 'rbnz', 'data']
        
        for pattern in priority_patterns:
            for file_path in excel_files:
                filename = os.path.basename(file_path).lower()
                if pattern in filename:
                    logger.info(f"Selected file based on pattern '{pattern}': {os.path.basename(file_path)}")
                    return file_path
        
        # If no priority match, use the most recent file
        most_recent = max(excel_files, key=os.path.getmtime)
        logger.info(f"Selected most recent file: {os.path.basename(most_recent)}")
        return most_recent

    def extract_description_pattern1(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> Optional[str]:
        """
        Structure example 1: Concatenate 3-6 rows into one string using ; as delimiter
        Expected: Series;Source;Seasonal adjustment;Units;Identifier
        """
        try:
            description_parts = []
            # Look in first 6 rows for description components
            for row in range(min(6, len(df))):
                cell_value = df.iloc[row, col_idx]
                if isinstance(cell_value, str) and len(cell_value.strip()) > 2:
                    description_parts.append(cell_value.strip())
            
            if len(description_parts) >= 3:
                return ';'.join(description_parts)
        except Exception as e:
            logger.debug(f"Pattern 1 extraction failed: {e}")
        return None

    def extract_description_pattern2(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> Optional[str]:
        """
        Structure example 2: Concatenate rows 1-3 (A column) using delimiter ; 
        and add them to concatenated rows 5-6 (columns with timeseries values)
        """
        try:
            # Get header info from column A (first 3 rows)
            header_parts = []
            for row in range(min(3, len(df))):
                if col_idx > 0:  # Only if we're not in column A
                    cell_value = df.iloc[row, 0]  # Column A
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 2:
                        header_parts.append(cell_value.strip())
            
            # Get specific column header
            column_header = None
            if col_idx < len(df.columns):
                # Look for column header in rows 4-6
                for row in range(4, min(7, len(df))):
                    cell_value = df.iloc[row, col_idx]
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 1:
                        column_header = cell_value.strip()
                        break
            
            if header_parts and column_header:
                return ';'.join(header_parts) + ';' + column_header
        except Exception as e:
            logger.debug(f"Pattern 2 extraction failed: {e}")
        return None

    def extract_description_pattern3(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> Optional[str]:
        """
        Structure example 3: Concatenate values from column B with ; delimiter 
        and add them to each concatenated timeseries column
        """
        try:
            # Get values from column B (index 1)
            if len(df.columns) > 1:
                column_b_parts = []
                for row in range(min(6, len(df))):
                    cell_value = df.iloc[row, 1]  # Column B
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 2:
                        column_b_parts.append(cell_value.strip())
                
                # Get current column header
                column_header = None
                for row in range(min(6, len(df))):
                    cell_value = df.iloc[row, col_idx]
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 1:
                        column_header = cell_value.strip()
                        break
                
                if column_b_parts and column_header:
                    return ';'.join(column_b_parts) + ';' + column_header
        except Exception as e:
            logger.debug(f"Pattern 3 extraction failed: {e}")
        return None

    def extract_description_pattern4(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> Optional[str]:
        """
        Structure example 4: Waterfall principle using separator ; for related headers/names
        """
        try:
            description_parts = []
            
            # Look for hierarchical structure in multiple columns
            for check_col in range(min(col_idx + 1, len(df.columns))):
                for row in range(min(8, len(df))):
                    cell_value = df.iloc[row, check_col]
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 3:
                        # Check if this looks like a header/category
                        if any(keyword in cell_value.lower() for keyword in ['figure', 'title', 'source', 'footnote']):
                            description_parts.append(cell_value.strip())
            
            if len(description_parts) >= 2:
                return ';'.join(description_parts)
        except Exception as e:
            logger.debug(f"Pattern 4 extraction failed: {e}")
        return None

    def extract_description_pattern5(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> Optional[str]:
        """
        Structure example 5: Concatenate headers and measure name using ; delimiter
        """
        try:
            # Look for structured headers above the data
            header_parts = []
            measure_name = None
            
            # Scan first 8 rows for headers and measure names
            for row in range(min(8, len(df))):
                for check_col in range(min(len(df.columns), col_idx + 2)):
                    cell_value = df.iloc[row, check_col]
                    if isinstance(cell_value, str) and len(cell_value.strip()) > 2:
                        clean_value = cell_value.strip()
                        # Check if it's a header (contains #, %, or descriptive text)
                        if any(char in clean_value for char in ['#', '%', '(', ')']):
                            if check_col <= col_idx:
                                header_parts.append(clean_value)
                        elif check_col == col_idx and not measure_name:
                            measure_name = clean_value
            
            if header_parts or measure_name:
                result_parts = header_parts
                if measure_name:
                    result_parts.append(measure_name)
                return ';'.join(result_parts)
        except Exception as e:
            logger.debug(f"Pattern 5 extraction failed: {e}")
        return None

    def extract_smart_description(self, df: pd.DataFrame, col_idx: int, sheet_name: str) -> str:
        """
        Attempts all 5 extraction patterns and returns the best match
        """
        patterns = [
            self.extract_description_pattern1,
            self.extract_description_pattern2,
            self.extract_description_pattern3,
            self.extract_description_pattern4,
            self.extract_description_pattern5
        ]
        
        best_description = None
        max_parts = 0
        
        for i, pattern_func in enumerate(patterns, 1):
            try:
                description = pattern_func(df, col_idx, sheet_name)
                if description:
                    parts_count = len(description.split(';'))
                    logger.debug(f"Pattern {i} for col {col_idx}: {parts_count} parts - {description[:100]}...")
                    if parts_count > max_parts:
                        max_parts = parts_count
                        best_description = description
            except Exception as e:
                logger.debug(f"Pattern {i} failed for col {col_idx}: {e}")
                continue
        
        # Fallback to simple description
        if not best_description:
            best_description = f"Data from sheet '{sheet_name}' column {col_idx + 1}"
        
        return best_description

    def generate_code(self, description: str) -> str:
        """
        Generate CBP.NZL code according to runbook specifications
        """
        # Clean and simplify description
        clean_desc = re.sub(r'[^\w\s;]', '', description)
        clean_desc = re.sub(r'\s+', ' ', clean_desc)
        
        # Split by semicolon and process each part
        parts = [part.strip() for part in clean_desc.split(';') if part.strip()]
        
        # Take meaningful words (length > 2) and convert to uppercase
        code_parts = []
        for part in parts[:6]:  # Limit to first 6 parts
            words = [word.upper() for word in part.split() if len(word) > 2]
            if words:
                code_parts.extend(words[:3])  # Max 3 words per part
        
        # Join with dots and create final code
        if code_parts:
            code_body = '.'.join(code_parts[:8])  # Limit total length
        else:
            code_body = 'UNKNOWN.SERIES'
        
        return f"CBP.NZL.{code_body}.Q"

    def determine_unit_info(self, description: str) -> Tuple[str, str, str]:
        """
        Determine UNIT_TYPE, DATA_TYPE, and DATA_UNIT from description
        """
        desc_lower = description.lower()
        
        # Check for percentage
        if any(word in desc_lower for word in ['%', 'percent', 'percentage']):
            return 'LEVEL', 'PERCENT', 'PERCENT'
        
        # Check for index
        if any(word in desc_lower for word in ['index', 'idx']):
            return 'LEVEL', 'INDEX', 'INDEX'
        
        # Check for currency
        if any(word in desc_lower for word in ['$', 'dollar', 'nzd', 'millions', 'currency']):
            return 'FLOW', 'CURRENCY', 'NZD'
        
        # Default
        return 'FLOW', 'UNITS', 'UNIT'

    def determine_multiplier(self, description: str) -> int:
        """
        Determine MULTIPLIER based on units in description
        """
        desc_lower = description.lower()
        
        if 'millions' in desc_lower:
            return 9
        elif any(word in desc_lower for word in ['000s', 'thousands']):
            return 3
        else:
            return 0

    def is_seasonally_adjusted(self, description: str) -> str:
        """
        Determine if series is seasonally adjusted
        """
        desc_lower = description.lower()
        if any(phrase in desc_lower for phrase in ['seasonally adjusted', 'seasonal adjustment']):
            return 'SA'
        return 'NSA'

    def format_date_to_quarter(self, date_value) -> str:
        """
        Convert date to YYYY-QN format as per runbook
        Handles Excel dates and various date formats including DD/MM/YYYY
        """
        try:
            if pd.isna(date_value):
                return None
            
            # Handle Excel datetime objects
            if isinstance(date_value, pd.Timestamp):
                date_obj = date_value.to_pydatetime()
            elif isinstance(date_value, datetime):
                date_obj = date_value
            elif isinstance(date_value, str):
                # Try to parse different date formats, prioritizing DD/MM/YYYY based on screenshot
                date_formats = [
                    '%d/%m/%Y',    # DD/MM/YYYY (31/03/2000)
                    '%m/%d/%Y',    # MM/DD/YYYY 
                    '%Y-%m-%d',    # YYYY-MM-DD
                    '%d-%m-%Y',    # DD-MM-YYYY
                    '%Y/%m/%d'     # YYYY/MM/DD
                ]
                
                date_str = date_value.strip()
                date_obj = None
                
                for fmt in date_formats:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if date_obj is None:
                    return None
            else:
                # Handle numeric values that might be Excel serial dates
                try:
                    # Only convert if it looks like a reasonable Excel serial date
                    # Excel dates typically range from 1900 (serial 1) to 2100+ (serial ~70000+)
                    if isinstance(date_value, (int, float)) and 1 <= date_value <= 80000:
                        # Excel epoch starts from 1900-01-01, but has a leap year bug
                        excel_epoch = datetime(1899, 12, 30)  # Adjust for Excel bug
                        date_obj = excel_epoch + pd.Timedelta(days=date_value)
                        
                        # Only accept if the result is in a reasonable year range
                        if 1990 <= date_obj.year <= 2030:
                            pass  # Accept this date
                        else:
                            return None
                    else:
                        return None
                except:
                    return None
            
            # Convert month to quarter
            quarter = ((date_obj.month - 1) // 3) + 1
            formatted_date = f"{date_obj.year}-Q{quarter}"
            
            # Debug log for verification
            logger.debug(f"Converted {date_value} to {formatted_date}")
            return formatted_date
        
        except Exception as e:
            logger.debug(f"Date formatting error for {date_value}: {e}")
            return None

    def scan_for_date_range(self, input_file: str) -> Tuple[str, str, List[str]]:
        """
        Pre-scan all sheets to determine the actual date range in the source data
        Returns: (earliest_period, latest_period, all_periods_found)
        """
        logger.info(f"Pre-scanning all sheets to determine date range in {os.path.basename(input_file)}...")
        
        try:
            excel_file = pd.ExcelFile(input_file, engine='openpyxl')
            all_periods = set()
            
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                    if df.empty:
                        continue
                    
                    # Look for dates in the first few columns
                    for col_idx in range(min(3, len(df.columns))):
                        for row_idx in range(len(df)):
                            cell_value = df.iloc[row_idx, col_idx]
                            formatted_date = self.format_date_to_quarter(cell_value)
                            if formatted_date:
                                all_periods.add(formatted_date)
                
                except Exception as e:
                    continue
            
            if all_periods:
                sorted_periods = sorted(list(all_periods))
                earliest = sorted_periods[0]
                latest = sorted_periods[-1]
                logger.info(f"Found date range: {earliest} to {latest} ({len(sorted_periods)} periods)")
                return earliest, latest, sorted_periods
            else:
                logger.warning("No dates found in source data, using default range")
                return "1990-Q1", "2030-Q4", []
                
        except Exception as e:
            logger.error(f"Error scanning for dates: {e}")
            return "1990-Q1", "2030-Q4", []

    def process_excel_file(self, input_file: str) -> Tuple[Dict, List, Dict, Dict, List[str]]:
        """
        Process the Excel file and extract time series data with advanced mapping
        Returns: (all_data, all_metadata, sheet_data, sheet_metadata, master_periods)
        """
        logger.info(f"Processing Excel file: {os.path.basename(input_file)}")
        
        try:
            # First, scan to determine the actual date range
            earliest_period, latest_period, master_periods = self.scan_for_date_range(input_file)
            
            excel_file = pd.ExcelFile(input_file, engine='openpyxl')
            all_data = {}
            all_metadata = []
            sheet_data = {}  # Data organized by sheet
            sheet_metadata = {}  # Metadata organized by sheet
            
            # Process sheets in their original order
            for sheet_name in excel_file.sheet_names:
                logger.info(f"Processing sheet: {sheet_name}")
                sheet_data[sheet_name] = {}
                sheet_metadata[sheet_name] = []
                
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                    if df.empty:
                        continue
                    
                    # Process each column for time series data
                    for col_idx in range(len(df.columns)):
                        series_data = self._extract_time_series_advanced(df, col_idx, sheet_name, master_periods)
                        if series_data:
                            # Add to overall data
                            all_data.update(series_data['data'])
                            all_metadata.extend(series_data['metadata'])
                            
                            # Add to sheet-specific data
                            sheet_data[sheet_name].update(series_data['data'])
                            sheet_metadata[sheet_name].extend(series_data['metadata'])
                
                except Exception as e:
                    logger.warning(f"Could not process sheet '{sheet_name}': {e}")
                    continue
            
            logger.info(f"Extracted {len(all_metadata)} time series from {len(excel_file.sheet_names)} sheets")
            return all_data, all_metadata, sheet_data, sheet_metadata, master_periods
            
        except Exception as e:
            logger.error(f"Failed to process Excel file: {e}")
            return {}, [], {}, {}, []

    def _extract_time_series_advanced(self, df: pd.DataFrame, col_idx: int, sheet_name: str, master_periods: List[str]) -> Optional[Dict]:
        """
        Advanced time series extraction with correct period mapping using master periods
        """
        try:
            # Get the full dataframe to work with
            if len(df.columns) <= col_idx:
                return None
                
            # Find numeric data and their corresponding dates
            numeric_data = {}  # period -> value mapping
            data_rows = []
            
            # Scan the entire column for numeric data
            for row_idx in range(len(df)):
                try:
                    cell_value = df.iloc[row_idx, col_idx]
                    numeric_val = pd.to_numeric(cell_value, errors='coerce')
                    
                    if not pd.isna(numeric_val):
                        # Find the corresponding date for this numeric value
                        date_found = None
                        
                        # Check multiple approaches to find the date
                        # 1. Same row, first few columns (most common)
                        for date_col in range(min(3, len(df.columns))):
                            if date_col != col_idx:  # Don't check the data column itself
                                date_cell = df.iloc[row_idx, date_col]
                                formatted_date = self.format_date_to_quarter(date_cell)
                                if formatted_date:
                                    date_found = formatted_date
                                    break
                        
                        # 2. If no date found, look in nearby rows (within 2 rows)
                        if not date_found:
                            for offset in [-1, 0, 1, 2]:
                                check_row = row_idx + offset
                                if 0 <= check_row < len(df):
                                    for date_col in range(min(3, len(df.columns))):
                                        if date_col != col_idx:
                                            date_cell = df.iloc[check_row, date_col]
                                            formatted_date = self.format_date_to_quarter(date_cell)
                                            if formatted_date:
                                                date_found = formatted_date
                                                break
                                    if date_found:
                                        break
                        
                        # Store if we found a valid date
                        if date_found:
                            numeric_data[date_found] = numeric_val
                            data_rows.append((row_idx, date_found, numeric_val))
                        else:
                            # Store row info for fallback mapping
                            data_rows.append((row_idx, None, numeric_val))
                
                except Exception as e:
                    continue
            
            # If we have no direct date matches, try sequential mapping
            if len(numeric_data) == 0 and len(data_rows) >= 8:
                logger.debug(f"No direct date matches in {sheet_name} col {col_idx}, trying sequential mapping")
                # Sort by row index and map sequentially to master periods
                data_rows.sort(key=lambda x: x[0])
                for i, (row_idx, _, numeric_val) in enumerate(data_rows):
                    if i < len(master_periods):
                        period = master_periods[i]
                        numeric_data[period] = numeric_val
            
            if len(numeric_data) < 8:
                return None
            
            # Create ordered arrays based on master periods
            numeric_values = []
            date_values = []
            
            for period in master_periods:
                if period in numeric_data:
                    numeric_values.append(numeric_data[period])
                    date_values.append(period)
                else:
                    # Add None for missing periods to maintain alignment
                    numeric_values.append(None)
                    date_values.append(period)
            
            # Remove trailing None values but keep at least 8 data points
            while len(numeric_values) > 8 and numeric_values[-1] is None:
                numeric_values.pop()
                date_values.pop()
            
            # Count actual numeric values (non-None)
            actual_values = [v for v in numeric_values if v is not None]
            if len(actual_values) < 8:
                return None
            
            # Extract description using smart pattern matching
            description = self.extract_smart_description(df, col_idx, sheet_name)
            code = self.generate_code(description)
            
            # Ensure unique code
            base_code = code
            counter = 1
            while code in [item.get('CODE') for item in getattr(self, '_processed_codes', [])]:
                code = f"{base_code[:-2]}.{counter}.Q"
                counter += 1
            
            # Store processed code
            if not hasattr(self, '_processed_codes'):
                self._processed_codes = []
            self._processed_codes.append({'CODE': code})
            
            # Determine metadata attributes
            unit_type, data_type, data_unit = self.determine_unit_info(description)
            multiplier = self.determine_multiplier(description)
            seasonally_adjusted = self.is_seasonally_adjusted(description)
            
            # Create metadata
            metadata = {
                'CODE': code,
                'CODE_MNEMONIC': code[:-2],  # Remove .Q
                'DESCRIPTION': description,
                'UNIT_TYPE': unit_type,
                'DATA_TYPE': data_type,
                'DATA_UNIT': data_unit,
                'SEASONALLY_ADJUSTED': seasonally_adjusted,
                'MULTIPLIER': multiplier,
                'LAST_RELEASE_DATE': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                **self.standard_metadata
            }
            
            return {
                'data': {code: numeric_values},
                'metadata': [metadata],
                'periods': date_values  # Include the actual periods for this series
            }
            
        except Exception as e:
            logger.debug(f"Failed to extract series from column {col_idx}: {e}")
            return None

    def create_qa_output(self, sheet_data: Dict, sheet_metadata: Dict, master_periods: List[str]) -> str:
        """
        Create QA output with each sheet mapped to its own Excel sheet
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        qa_path = os.path.join(self.output_dir, f"CBP_QA_OUTPUT_{timestamp}.xlsx")
        
        try:
            with pd.ExcelWriter(qa_path, engine='openpyxl') as writer:
                for sheet_name, data in sheet_data.items():
                    if not data:
                        continue
                    
                    metadata = sheet_metadata.get(sheet_name, [])
                    
                    # Create sheet-specific data structure
                    sheet_final_data = {}
                    
                    # Find max length for this sheet
                    max_length = max(len(values) for values in data.values()) if data else 0
                    
                    # Use the master periods for correct period alignment
                    period_column = [None]  # Description row
                    if master_periods:
                        period_column.extend(master_periods)
                    
                    sheet_final_data['PERIOD'] = period_column
                    
                    # Add each time series for this sheet
                    for meta in metadata:
                        code = meta['CODE']
                        if code in data:
                            column_data = [meta['DESCRIPTION']]
                            data_values = data[code][:]
                            # Pad to match master periods length
                            while len(data_values) < len(master_periods):
                                data_values.append(None)
                            column_data.extend(data_values)
                            sheet_final_data[code] = column_data
                    
                    # Create DataFrame for this sheet
                    if sheet_final_data:
                        sheet_df = pd.DataFrame(sheet_final_data)
                        
                        # Clean sheet name for Excel (remove invalid characters)
                        clean_sheet_name = re.sub(r'[\\/*?[\]:]+', '_', str(sheet_name))[:31]
                        sheet_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                        
                        logger.info(f"Created QA sheet: {clean_sheet_name} with {len(data)} series")
            
            logger.info(f"Created QA output file: {os.path.basename(qa_path)}")
            return qa_path
            
        except Exception as e:
            logger.error(f"Error creating QA output: {e}")
            return None

    def create_final_output(self, all_data: Dict, all_metadata: List, master_periods: List[str]) -> str:
        """
        Create CBP format files following exact CBP_NZL_DATA structure
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not all_data or not all_metadata:
            logger.error("Cannot create CBP files - no data extracted")
            return None
        
        try:
            # Create DATA file in exact CBP_NZL_DATA format
            data_path = os.path.join(self.output_dir, f"CBP_DATA_{timestamp}.xlsx")
            
            # Find maximum data length to determine number of quarters
            max_length = max(len(values) for values in all_data.values()) if all_data else 0
            
            # Create the data structure exactly like CBP_NZL_DATA
            final_data = {}
            
            # First column: periods (None for description row, then actual periods from source)
            period_column = [None]  # Description row gets None
            if master_periods:
                period_column.extend(master_periods)
            
            final_data['PERIOD'] = period_column
            
            # Add each time series as a column
            for metadata in all_metadata:
                code = metadata['CODE']
                if code in all_data:
                    # First row: description, then data
                    column_data = [metadata['DESCRIPTION']]  # Description in first row
                    data_values = all_data[code]
                    
                    # Pad data to match master periods length
                    while len(data_values) < len(master_periods):
                        data_values.append(None)
                    
                    column_data.extend(data_values)
                    final_data[code] = column_data
            
            # Create DataFrame and save (without index and without header row names)
            final_df = pd.DataFrame(final_data)
            final_df.to_excel(data_path, index=False, header=True, engine='openpyxl')
            logger.info(f"Created DATA file in CBP_NZL_DATA format: {os.path.basename(data_path)}")
            
            # Create METADATA file (unchanged)
            meta_path = os.path.join(self.output_dir, f"CBP_META_{timestamp}.xlsx")
            meta_df = pd.DataFrame(all_metadata)
            meta_df.to_excel(meta_path, index=False, engine='openpyxl')
            logger.info(f"Created METADATA file: {os.path.basename(meta_path)}")
            
            # Create ZIP archive
            zip_path = os.path.join(self.output_dir, f"CBP_{timestamp}.zip")
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(data_path, os.path.basename(data_path))
                zipf.write(meta_path, os.path.basename(meta_path))
            
            logger.info(f"Created ZIP archive: {os.path.basename(zip_path)}")
            return zip_path
            
        except Exception as e:
            logger.error(f"Error creating CBP files: {e}")
            return None

    def run(self) -> Tuple[str, str]:
        """
        Run the complete mapping process and create both QA and Final outputs
        """
        logger.info("Starting RBNZ CBP mapping process...")
        
        try:
            # Find Excel files to process
            excel_files = self.find_excel_files()
            
            if not excel_files:
                logger.error("No Excel files found in downloads directory")
                return None, None
            
            # Select file to process
            selected_file = self.select_file_to_process(excel_files)
            
            if not selected_file:
                logger.error("No suitable Excel file found")
                return None, None
            
            logger.info(f"Processing file: {os.path.basename(selected_file)}")
            
            # Process Excel file with period detection
            all_data, all_metadata, sheet_data, sheet_metadata, master_periods = self.process_excel_file(selected_file)
            
            if not all_data:
                logger.error("No time series data extracted")
                return None, None
            
            logger.info(f"Using master period range: {master_periods[0] if master_periods else 'N/A'} to {master_periods[-1] if master_periods else 'N/A'}")
            
            # Create QA output (each sheet in separate Excel sheet)
            qa_path = self.create_qa_output(sheet_data, sheet_metadata, master_periods)
            
            # Create Final output (all data consolidated)
            final_path = self.create_final_output(all_data, all_metadata, master_periods)
            
            if qa_path and final_path:
                logger.info("Mapping process completed successfully!")
                logger.info(f"QA Output: {qa_path}")
                logger.info(f"Final Output: {final_path}")
                return qa_path, final_path
            else:
                logger.error("Failed to create output files")
                return qa_path, final_path
                
        except Exception as e:
            logger.error(f"Fatal error in mapping process: {e}")
            return None, None


if __name__ == "__main__":
    # Dynamic file processing - scans downloads directory for Excel files
    downloads_dir = "./downloads"  # Directory to scan for Excel files
    output_dir = "./mapped_output"  # Output directory
    
    mapper = RBNZMapper(downloads_dir, output_dir)
    qa_result, final_result = mapper.run()
    
    if qa_result and final_result:
        print(f"\nSuccess! Both outputs created:")
        print(f"QA Output: {qa_result}")
        print(f"Final Output: {final_result}")
    elif qa_result or final_result:
        print(f"\nPartial success:")
        if qa_result:
            print(f"QA Output: {qa_result}")
        if final_result:
            print(f"Final Output: {final_result}")
    else:
        print(f"\nMapping failed. Check logs for details.")