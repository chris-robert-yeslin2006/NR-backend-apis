from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from enum import Enum

class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    columns: List[str]
    row_count: int
    file_size: int
    upload_time: str

class NodeType(str, Enum):
    FILE = "file"
    CSV = "csv"
    SAMPLER = "sampler"
    SELECT_COLUMNS = "select-columns"
    SELECT_ROWS = "select-rows"
    FILTER_MORE = "filter-more"
    SCATTER_PLOT = "scatter-plot"
    LINE_PLOT = "line-plot"
    BAR_PLOT = "bar-plot"
    BOX_PLOT = "box-plot"

class NodeData(BaseModel):
    node_id: str
    node_type: NodeType
    parameters: Dict[str, Any] = {}
    file_id: Optional[str] = None
    parent_nodes: List[str] = []

class VisualizationRequest(BaseModel):
    node_id: str
    visualization_type: str
    parameters: Dict[str, Any]
    data_source_file_id: Optional[str] = None

class VisualizationResponse(BaseModel):
    visualization_id: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]