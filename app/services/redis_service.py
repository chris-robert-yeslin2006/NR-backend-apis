import redis.asyncio as redis
import pickle
from typing import Any, Optional, List
from app.config import settings
import logging

logger = logging.getLogger(__name__)

class RedisService:
    def __init__(self):
        self.redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=False  # keep raw bytes, since we pickle
        )
    
    async def store_file_data(self, file_id: str, data: dict, expiry: int = settings.temp_file_expiry) -> bool:
        """Store file data with expiration"""
        try:
            serialized_data = pickle.dumps(data)
            await self.redis_client.setex(f"file:{file_id}", expiry, serialized_data)
            logger.info(f"Stored file data for file_id: {file_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing file data: {str(e)}")
            return False
    
    async def get_file_data(self, file_id: str) -> Optional[dict]:
        """Retrieve file data"""
        try:
            serialized_data = await self.redis_client.get(f"file:{file_id}")
            if serialized_data:
                return pickle.loads(serialized_data)
            return None
        except Exception as e:
            logger.error(f"Error retrieving file data: {str(e)}")
            return None
    
    async def store_processed_data(self, node_id: str, data: List[dict], expiry: int = 1800) -> bool:
        """Store processed node data"""
        try:
            serialized_data = pickle.dumps(data)
            await self.redis_client.setex(f"node:{node_id}", expiry, serialized_data)
            return True
        except Exception as e:
            logger.error(f"Error storing processed data: {str(e)}")
            return False
    
    async def get_processed_data(self, node_id: str) -> Optional[List[dict]]:
        """Retrieve processed node data"""
        try:
            serialized_data = await self.redis_client.get(f"node:{node_id}")
            if serialized_data:
                return pickle.loads(serialized_data)
            return None
        except Exception as e:
            logger.error(f"Error retrieving processed data: {str(e)}")
            return None
    
    async def delete_file_data(self, file_id: str) -> bool:
        """Delete file data"""
        try:
            await self.redis_client.delete(f"file:{file_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting file data: {str(e)}")
            return False


redis_service = RedisService()
