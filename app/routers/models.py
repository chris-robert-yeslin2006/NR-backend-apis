from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import classification_report
from app.services.redis_service import redis_service

router = APIRouter(prefix="/api/models", tags=["models"])

class ModelRequest(BaseModel):
    file_id: str
    target_column: str            # Name of the label/target column
    model_type: str              # 'knn', 'random_forest', 'adaboost', 'naive_bayes'
    parameters: Optional[Dict[str, Any]] = None

@router.post("/train")
async def train_model(req: ModelRequest):
    # Load data from Redis cache
    file_data = await redis_service.get_file_data(req.file_id)
    if not file_data:
        raise HTTPException(status_code=404, detail="File data not found")

    df = pd.DataFrame(file_data['data'])
    
    # Basic checks
    if req.target_column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Target column '{req.target_column}' not found")
    
    # Prepare features and labels
    X = df.drop(columns=[req.target_column])
    y = df[req.target_column]

    # Encode non-numeric columns
    X = X.apply(lambda col: LabelEncoder().fit_transform(col) if col.dtype == 'object' else col)
    y = LabelEncoder().fit_transform(y)

    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    model_type = req.model_type.lower()
    params = req.parameters or {}

    # Initialize and train model
    if model_type == "knn":
        model = KNeighborsClassifier(**params)
    elif model_type == "random_forest":
        model = RandomForestClassifier(**params)
    elif model_type == "adaboost":
        model = AdaBoostClassifier(**params)
    elif model_type == "naive_bayes":
        model = GaussianNB()
    else:
        raise HTTPException(status_code=400, detail="Unsupported model type")

    model.fit(X_train, y_train)

    # Predict on test set
    y_pred = model.predict(X_test)

    # Generate classification report
    report = classification_report(y_test, y_pred, output_dict=True)

    # Feature importance for applicable models
    feature_importances = None
    if model_type in ["random_forest", "adaboost"]:
        feature_importances = dict(zip(X.columns, model.feature_importances_))
    elif model_type == "knn":
        feature_importances = "Feature importances not available"
    elif model_type == "naive_bayes":
        feature_importances = "Feature importances not generally available"

    # Compose response
    return {
        "model_type": model_type,
        "classification_report": report,
        "feature_importances": feature_importances
    }
