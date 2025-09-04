from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.file_service import file_service
from app.models.schemas import FileUploadResponse
from app.services.redis_service import redis_service

router = APIRouter(prefix="/api/files", tags=["files"])

@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    """Upload and process a data file"""
    return await file_service.process_uploaded_file(file)

@router.get("/{file_id}/data")
async def get_file_data(file_id: str, limit: int = 1000):
    """Get file data by file ID"""
    file_data = await redis_service.get_file_data(file_id)
    print(file_data)
    
    if not file_data:
        raise HTTPException(status_code=404, detail="File not found")
    
    data = file_data.get('data', [])
    print(data)
    return {
        'file_id': file_id,
        'data': data[:limit],
        'total_rows': len(data),
        'columns': file_data.get('columns', []),
        'metadata': {
            'filename': file_data.get('filename'),
            'upload_time': file_data.get('upload_time'),
            'file_size': file_data.get('file_size')
        }
    }

@router.delete("/{file_id}")
async def delete_file(file_id: str):
    """Delete a file and its data"""
    success = await redis_service.delete_file_data(file_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="File not found")
    
    return {"message": "File deleted successfully"}