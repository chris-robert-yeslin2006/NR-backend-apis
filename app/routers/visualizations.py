from fastapi import APIRouter, HTTPException
from app.models.schemas import VisualizationRequest, VisualizationResponse
from app.services.visualization_service import visualization_service
from app.services.redis_service import redis_service
import uuid

router = APIRouter(prefix="/api/visualizations", tags=["visualizations"])

@router.post("/generate", response_model=VisualizationResponse)
async def generate_visualization(request: VisualizationRequest):
    """Generate visualization data"""
    try:
        # Get data for visualization
        if request.data_source_file_id:
            file_data = await redis_service.get_file_data(request.data_source_file_id)
            if not file_data:
                raise HTTPException(status_code=404, detail="Source file not found")
            data = file_data.get('data', [])
        else:
            # Get data from processed node
            data = await redis_service.get_processed_data(request.node_id)
            if not data:
                raise HTTPException(status_code=404, detail="Node data not found")
        
        # Generate visualization based on type
        if request.visualization_type == 'scatter-plot':
            viz_result = await visualization_service.generate_scatter_plot_data(data, request.parameters)
        elif request.visualization_type == 'line-plot':
            viz_result = await visualization_service.generate_line_plot_data(data, request.parameters)
        elif request.visualization_type == 'bar-plot':
            viz_result = await visualization_service.generate_bar_plot_data(data, request.parameters)
        elif request.visualization_type == 'box-plot':
            viz_result = await visualization_service.generate_box_plot_data(data, request.parameters)
        else:
            raise HTTPException(status_code=400, detail="Unsupported visualization type")
        
        visualization_id = str(uuid.uuid4())
        
        return VisualizationResponse(
            visualization_id=visualization_id,
            data=viz_result['data'],
            metadata=viz_result['metadata']
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{visualization_id}")
async def get_visualization(visualization_id: str):
    """Get cached visualization data"""
    # This could be extended to cache visualization results
    raise HTTPException(status_code=501, detail="Not implemented - use generate endpoint")