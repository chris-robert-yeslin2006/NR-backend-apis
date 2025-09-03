from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
from app.models.schemas import NodeData
from app.services.data_processing import data_processing_service
from app.services.redis_service import redis_service

router = APIRouter(prefix="/api/nodes", tags=["nodes"])

@router.post("/{node_id}/process")
async def process_node(node_id: str, node_data: NodeData):
    """Process data through a node"""
    try:
        # Get input data based on node type and file_id
        if node_data.file_id:
            file_data = await redis_service.get_file_data(node_data.file_id)
            if not file_data:
                raise HTTPException(status_code=404, detail="Source file not found")
            input_data = file_data.get('data', [])
        else:
            # Get data from parent nodes
            input_data = []
            for parent_id in node_data.parent_nodes:
                parent_data = await redis_service.get_processed_data(parent_id)
                if parent_data:
                    input_data.extend(parent_data)
        
        # Process the data
        processed_data = await data_processing_service.process_node_data(
            node_data.dict(), input_data
        )
        
        # Store processed data
        await redis_service.store_processed_data(node_id, processed_data)
        
        return {
            'node_id': node_id,
            'status': 'success',
            'row_count': len(processed_data),
            'columns': list(processed_data[0].keys()) if processed_data else [],
            'sample_data': processed_data[:5]  # First 5 rows as sample
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{node_id}/data")
async def get_node_data(node_id: str, limit: int = 1000):
    """Get processed data from a node"""
    data = await redis_service.get_processed_data(node_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="Node data not found")
    
    return {
        'node_id': node_id,
        'data': data[:limit],
        'total_rows': len(data),
        'columns': list(data[0].keys()) if data else []
    }