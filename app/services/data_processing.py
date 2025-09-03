import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
from app.services.redis_service import redis_service

logger = logging.getLogger(__name__)

class DataProcessingService:
    @staticmethod
    async def process_node_data(node_data: Dict[str, Any], input_data: List[Dict]) -> List[Dict]:
        """Process data based on node type and parameters"""
        node_type = node_data['node_type']
        parameters = node_data.get('parameters', {})
        
        try:
            if node_type == 'sampler':
                return DataProcessingService._apply_sampler(input_data, parameters)
            elif node_type == 'select-columns':
                return DataProcessingService._apply_column_selection(input_data, parameters)
            elif node_type == 'select-rows':
                return DataProcessingService._apply_row_selection(input_data, parameters)
            elif node_type == 'filter-more':
                return DataProcessingService._apply_filter(input_data, parameters)
            else:
                return input_data
                
        except Exception as e:
            logger.error(f"Error processing node data: {str(e)}")
            raise ValueError(f"Data processing error: {str(e)}")
    
    @staticmethod
    def _apply_sampler(data: List[Dict], parameters: Dict) -> List[Dict]:
        """Apply sampling to data"""
        sample_size = min(parameters.get('sampleSize', 100), len(data))
        method = parameters.get('method', 'random')
        
        if method == 'random':
            df = pd.DataFrame(data)
            sampled = df.sample(n=sample_size, random_state=parameters.get('seed', 42))
            return sampled.to_dict('records')
        elif method == 'systematic':
            step = len(data) // sample_size if sample_size > 0 else 1
            return data[::step][:sample_size]
        else:
            return data[:sample_size]
    
    @staticmethod
    def _apply_column_selection(data: List[Dict], parameters: Dict) -> List[Dict]:
        """Select specific columns from data"""
        if not data:
            return data
            
        columns_param = parameters.get('columns', '')
        if not columns_param:
            return data
        
        selected_columns = [col.strip() for col in columns_param.split(',')]
        available_columns = list(data[0].keys())
        valid_columns = [col for col in selected_columns if col in available_columns]
        
        if not valid_columns:
            return data
        
        return [{col: row.get(col) for col in valid_columns} for row in data]
    
    @staticmethod
    def _apply_row_selection(data: List[Dict], parameters: Dict) -> List[Dict]:
        """Select specific rows from data"""
        start_row = parameters.get('startRow', 0)
        end_row = parameters.get('endRow', len(data))
        
        return data[start_row:end_row]
    
    @staticmethod
    def _apply_filter(data: List[Dict], parameters: Dict) -> List[Dict]:
        """Apply filtering to data"""
        column = parameters.get('column')
        operator = parameters.get('operator')
        value = parameters.get('value')
        
        if not all([column, operator, value is not None]):
            return data
        
        filtered_data = []
        
        for row in data:
            if column not in row:
                continue
                
            cell_value = row[column]
            
            try:
                if operator == '==':
                    if str(cell_value) == str(value):
                        filtered_data.append(row)
                elif operator == '!=':
                    if str(cell_value) != str(value):
                        filtered_data.append(row)
                elif operator == '>':
                    if float(cell_value) > float(value):
                        filtered_data.append(row)
                elif operator == '<':
                    if float(cell_value) < float(value):
                        filtered_data.append(row)
                elif operator == '>=':
                    if float(cell_value) >= float(value):
                        filtered_data.append(row)
                elif operator == '<=':
                    if float(cell_value) <= float(value):
                        filtered_data.append(row)
                elif operator == 'contains':
                    if str(value) in str(cell_value):
                        filtered_data.append(row)
                elif operator == 'startswith':
                    if str(cell_value).startswith(str(value)):
                        filtered_data.append(row)
            except (ValueError, TypeError):
                continue
        
        return filtered_data

data_processing_service = DataProcessingService()