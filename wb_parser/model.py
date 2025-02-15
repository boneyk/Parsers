from pydantic import BaseModel, field_validator
from typing import Optional, List, Dict, Any

# ["id", "название", "бренд", "цена (руб.)", "стоимость доставки", "рейтинг", "количество отзывов", "в наличии","количество картинок","уровень продавца","рейтинг продавца","расстояние до товара","ставка за тысячу показов","участвует в продвижении?", "место товара при продвижении","место товара без продвижения", "количество цветов"]
class Item(BaseModel):
    id: int
    name: str
    brand: str
    reviewRating: float
    feedbacks: int
    volume: int
    totalQuantity: int
    viewFlags: int
    supplierFlags: int
    pics: int
    supplierRating: float
    dist: int
    promoTextCard: Optional[str] = None
    sizes: Optional[List[Dict[str, Any]]] = None  # Список размеров, содержащий цену
    log: Optional[Dict[str, Any]] = None  # Список логов, содержащий маркетинговую и техническую информацию
    colors: Optional[List[Dict[str, Any]]] = None  # Список цветов товара


class Items(BaseModel):
    products: List[Item]
