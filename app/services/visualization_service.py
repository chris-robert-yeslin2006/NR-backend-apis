import pandas as pd
import numpy as np
from typing import List, Dict, Any
import uuid
import logging
from app.services.redis_service import redis_service

logger = logging.getLogger(__name__)

class VisualizationService:
    @staticmethod
    async def generate_scatter_plot_data(data: List[Dict], parameters: Dict) -> Dict[str, Any]:
        """Generate data for scatter plot visualization"""
        try:
            df = pd.DataFrame(data)
            
            x_axis = parameters.get('xAxis', df.columns[0] if len(df.columns) > 0 else 'x')
            y_axis = parameters.get('yAxis', df.columns[1] if len(df.columns) > 1 else 'y')
            color_by = parameters.get('colorBy')
            
            # Ensure columns exist
            if x_axis not in df.columns or y_axis not in df.columns:
                raise ValueError(f"Columns {x_axis} or {y_axis} not found in data")
            
            # Prepare visualization data
            viz_data = []
            for _, row in df.iterrows():
                point = {
                    'x': row[x_axis],
                    'y': row[y_axis],
                    'index': len(viz_data)
                }
                
                if color_by and color_by in row:
                    point['category'] = row[color_by]
                
                viz_data.append(point)
            
            metadata = {
                'x_axis': x_axis,
                'y_axis': y_axis,
                'color_by': color_by,
                'point_count': len(viz_data),
                'x_range': [float(df[x_axis].min()), float(df[x_axis].max())] if pd.api.types.is_numeric_dtype(df[x_axis]) else None,
                'y_range': [float(df[y_axis].min()), float(df[y_axis].max())] if pd.api.types.is_numeric_dtype(df[y_axis]) else None
            }
            
            return {
                'data': viz_data,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error generating scatter plot data: {str(e)}")
            raise ValueError(f"Scatter plot generation error: {str(e)}")
    
    @staticmethod
    async def generate_line_plot_data(data: List[Dict], parameters: Dict) -> Dict[str, Any]:
        """Generate data for line plot visualization"""
        try:
            df = pd.DataFrame(data)
            
            x_axis = parameters.get('xAxis', df.columns[0] if len(df.columns) > 0 else 'x')
            y_axis = parameters.get('yAxis', df.columns[1] if len(df.columns) > 1 else 'y')
            
            if x_axis not in df.columns or y_axis not in df.columns:
                raise ValueError(f"Columns {x_axis} or {y_axis} not found in data")
            
            # Sort by x-axis for line plot
            df_sorted = df.sort_values(by=x_axis)
            
            viz_data = []
            for _, row in df_sorted.iterrows():
                viz_data.append({
                    'x': row[x_axis],
                    'y': row[y_axis]
                })
            
            metadata = {
                'x_axis': x_axis,
                'y_axis': y_axis,
                'point_count': len(viz_data),
                'line_color': parameters.get('lineColor', '#8884d8')
            }
            
            return {
                'data': viz_data,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error generating line plot data: {str(e)}")
            raise ValueError(f"Line plot generation error: {str(e)}")
    
    @staticmethod
    async def generate_bar_plot_data(data: List[Dict], parameters: Dict) -> Dict[str, Any]:
        """Generate data for bar plot visualization"""
        try:
            df = pd.DataFrame(data)
            
            x_axis = parameters.get('xAxis', df.columns[0] if len(df.columns) > 0 else 'category')
            y_axis = parameters.get('yAxis', df.columns[1] if len(df.columns) > 1 else 'value')
            
            if x_axis not in df.columns or y_axis not in df.columns:
                raise ValueError(f"Columns {x_axis} or {y_axis} not found in data")
            
            # Group by x_axis and aggregate y_axis values
            grouped = df.groupby(x_axis)[y_axis].sum().reset_index()
            
            viz_data = []
            for _, row in grouped.iterrows():
                viz_data.append({
                    'category': row[x_axis],
                    'value': row[y_axis]
                })
            
            metadata = {
                'x_axis': x_axis,
                'y_axis': y_axis,
                'bar_count': len(viz_data),
                'bar_color': parameters.get('barColor', '#8884d8')
            }
            
            return {
                'data': viz_data,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error generating bar plot data: {str(e)}")
            raise ValueError(f"Bar plot generation error: {str(e)}")
    
    @staticmethod
    async def generate_box_plot_data(data: List[Dict], parameters: Dict) -> Dict[str, Any]:
        """Generate data for box plot visualization"""
        try:
            df = pd.DataFrame(data)
            
            column = parameters.get('column', df.columns[0] if len(df.columns) > 0 else 'value')
            group_by = parameters.get('groupBy')
            
            if column not in df.columns:
                raise ValueError(f"Column {column} not found in data")
            
            viz_data = []
            
            if group_by and group_by in df.columns:
                # Grouped box plot
                for group_name in df[group_by].unique():
                    group_data = df[df[group_by] == group_name][column].dropna()
                    
                    if len(group_data) > 0:
                        viz_data.append({
                            'group': group_name,
                            'values': group_data.tolist(),
                            'q1': float(group_data.quantile(0.25)),
                            'median': float(group_data.median()),
                            'q3': float(group_data.quantile(0.75)),
                            'min': float(group_data.min()),
                            'max': float(group_data.max())
                        })
            else:
                # Single box plot
                values = df[column].dropna()
                
                if len(values) > 0:
                    viz_data.append({
                        'group': 'All Data',
                        'values': values.tolist(),
                        'q1': float(values.quantile(0.25)),
                        'median': float(values.median()),
                        'q3': float(values.quantile(0.75)),
                        'min': float(values.min()),
                        'max': float(values.max())
                    })
            
            metadata = {
                'column': column,
                'group_by': group_by,
                'box_count': len(viz_data),
                'show_outliers': parameters.get('showOutliers', True)
            }
            
            return {
                'data': viz_data,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"Error generating box plot data: {str(e)}")
            raise ValueError(f"Box plot generation error: {str(e)}")

visualization_service = VisualizationService()