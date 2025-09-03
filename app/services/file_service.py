import pandas as pd
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Tuple
from fastapi import UploadFile, HTTPException
import logging
from app.services.redis_service import redis_service
from app.config import settings

logger = logging.getLogger(__name__)

class FileService:
    @staticmethod
    async def process_uploaded_file(file: UploadFile) -> Dict[str, Any]:
        """Process uploaded file and return file data with metadata"""
        
        # Validate file extension
        file_extension = file.filename.split('.')[-1].lower()
        if file_extension not in settings.allowed_file_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"File type not supported. Allowed: {', '.join(settings.allowed_file_extensions)}"
            )
        
        # Check file size
        content = await file.read()
        if len(content) > settings.file_upload_max_size:
            raise HTTPException(
                status_code=400,
                detail="File too large"
            )
        
        file_id = str(uuid.uuid4())
        
        try:
            # Process based on file type
            if file_extension == 'csv':
                data, columns = await FileService._process_csv(content)
                print(data)
                print(columns)
            elif file_extension == 'json':
                data, columns = await FileService._process_json(content)
                print(data)
                print(columns)
            elif file_extension in ['xlsx', 'xls']:
                data, columns = await FileService._process_excel(content)
                print(data)
                print(columns)
            elif file_extension == 'txt':
                data, columns = await FileService._process_text(content)
                print(data)
                print(columns)
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")
            
            file_metadata = {
                'file_id': file_id,
                'filename': file.filename,
                'file_size': len(content),
                'columns': columns,
                'row_count': len(data),
                'upload_time': datetime.now().isoformat(),
                'data': data
            }
            
            # Store in Redis
            await redis_service.store_file_data(file_id, file_metadata)
            
            return {
                'file_id': file_id,
                'filename': file.filename,
                'columns': columns,
                'row_count': len(data),
                'file_size': len(content),
                'upload_time': file_metadata['upload_time']
            }
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    
    @staticmethod
    async def _process_csv(content: bytes) -> Tuple[List[Dict], List[str]]:
        """Process CSV file"""
        try:
            df = pd.read_csv(pd.io.common.StringIO(content.decode('utf-8')))
            columns = df.columns.tolist()
            data = df.to_dict('records')
            return data, columns
        except Exception as e:
            raise ValueError(f"Error processing CSV: {str(e)}")
    
    @staticmethod
    async def _process_json(content: bytes) -> Tuple[List[Dict], List[str]]:
        """Process JSON file"""
        try:
            json_data = json.loads(content.decode('utf-8'))
            if isinstance(json_data, list) and len(json_data) > 0:
                columns = list(json_data[0].keys()) if json_data else []
                return json_data, columns
            else:
                raise ValueError("JSON must be an array of objects")
        except Exception as e:
            raise ValueError(f"Error processing JSON: {str(e)}")
    
    @staticmethod
    async def _process_excel(content: bytes) -> Tuple[List[Dict], List[str]]:
        """Process Excel file"""
        try:
            df = pd.read_excel(pd.io.common.BytesIO(content))
            columns = df.columns.tolist()
            data = df.to_dict('records')
            return data, columns
        except Exception as e:
            raise ValueError(f"Error processing Excel: {str(e)}")
    
    @staticmethod
    async def _process_text(content: bytes) -> Tuple[List[Dict], List[str]]:
        """Process text file"""
        try:
            lines = content.decode('utf-8').split('\n')
            columns = ['line_number', 'content']
            data = [{'line_number': i + 1, 'content': line.strip()} 
                   for i, line in enumerate(lines) if line.strip()]
            return data, columns
        except Exception as e:
            raise ValueError(f"Error processing text file: {str(e)}")

file_service = FileService()