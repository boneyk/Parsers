from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any


class Price(BaseModel):
    total: Optional[float] = None


class Item(BaseModel):
    id: int
    name: str
    brand: str
    reviewRating: float
    feedbacks: int
    volume: int
    sizes: Optional[List[Dict[str, Any]]] = None  # Список размеров, содержащий цену


class Items(BaseModel):
    products: List[Item]
