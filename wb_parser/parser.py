import openpyxl
import csv
from model import Items
from datetime import datetime
from clickhouse_driver import Client
import requests
from typing import Optional, Dict, List, Tuple
import logging

LAST_WRITE_TIMES = {}

class ParserWB:
    def __init__(self, query: str):
        # Используем основной логгер бота
        self.logger = logging.getLogger('WBTrackerBot')
        self.query = query
        self.filename_excel = f"{query}.xlsx"
        self.filename_csv = f"{query}.csv"
        
        try:
            self.client = Client(
                host='195.133.194.116',
                port=9000,  
                user='default',
                password='1234',
                settings={'connect_timeout': 10}
            )
            
            test_result = self.client.execute('SELECT 1')
            self.logger.info(f"[ParserWB] Соединение с ClickHouse установлено успешно (запрос: '{query}')")
        
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка подключения к ClickHouse: {str(e)}", exc_info=True)
            raise

    def _execute_query(self, query, params=None):
        """Выполняет SQL-запрос к ClickHouse через clickhouse-driver"""
        try:
            self.logger.debug(f"[ParserWB] Выполнение запроса: {query[:100]}... (params: {params})")
            result = self.client.execute(query, params)
            self.logger.debug("[ParserWB] Запрос выполнен успешно")
            return result
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка выполнения запроса: {str(e)}", exc_info=True)
            return None

    def parse(self):
        _page = 1
        all_items = Items(products=[])
        self.logger.info(f"[ParserWB] Начало парсинга запроса: '{self.query}'")

        while _page < 61:
            params = {
                'ab_testing': 'false',
                'appType': '1',
                'curr': 'rub',
                'dest': '123585528',
                'lang': 'ru',
                'page': _page,
                'query': self.query,
                'resultset': 'catalog',
                'sort': 'popular',
                'spp': '30',
                'suppressSpellcheck': 'false',
            }

            self.logger.debug(f"[ParserWB] Запрос страницы {_page} с параметрами: {params}")
            response = requests.get('https://search.wb.ru/exactmatch/ru/common/v9/search', params=params)
            _page += 1
            
            if response.status_code == 200:
                data = response.json().get("data", {})
                
                if "products" in data and isinstance(data["products"], list) and data["products"]:
                    items_info = Items.model_validate(data)
                    all_items.products.extend(items_info.products)
                    self.logger.info(f"[ParserWB] Прочитаны данные со страницы {_page-1}: {len(items_info.products)} товаров")
                else:
                    self.logger.warning(f"[ParserWB] Список товаров пуст или отсутствует на странице {_page-1}")
                    break
            else:
                self.logger.error(f"[ParserWB] Ошибка при запросе данных! Код: {response.status_code}")
                break
        
        if all_items.products:
            self.logger.info(f"[ParserWB] Всего загружено {len(all_items.products)} товаров. Сохранение в ClickHouse...")
            result = self.__save_to_db(all_items)
            if result:
                self.logger.info(f"[ParserWB] Успешно загружено {len(all_items.products)} записей в ClickHouse")
            else:
                self.logger.error("[ParserWB] Ошибка при загрузке данных в ClickHouse")
            return all_items
        else:
            self.logger.warning("[ParserWB] Не найдено ни одного товара по запросу")
            return None

    def find_product_position(self, article: int, query: str) -> Optional[Dict]:
        """
        Ищет товар по артикулу в текущем запросе и возвращает его позицию и данные
        Возвращает словарь с ключами: position, product_data
        """
        try:
            self.logger.info(f"[ParserWB] Поиск товара {article} по запросу '{query}'")
            parsed_data = self.parse()
            
            if parsed_data:
                for idx, product in enumerate(parsed_data.products):
                    if product.id == article:
                        position = idx + 1
                        self.logger.info(f"[ParserWB] Товар {article} найден на позиции {position}")
                        return {
                            'position': position,
                            'product_data': self._extract_product_data(product)
                        }
            
            self.logger.warning(f"[ParserWB] Товар {article} не найден в текущем парсинге, поиск в БД")
            return self._find_product_in_db(article, query)
            
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка при поиске товара {article}: {str(e)}", exc_info=True)
            return None

    def _find_product_in_db(self, article: int, query: str) -> Optional[Dict]:
        """
        Ищет товар в ClickHouse по артикулу и запросу
        Возвращает последнюю запись о товаре
        """
        query_sql = """
        SELECT 
            if (promotion = 'no promotion' or promotion = '',position,promoPosition) as position_with_promo,
            name,
            brand,
            price,
            number_of_feedbacks,
            reviewRating,
            promoTextCard,
            created_at
        FROM wildberries.data
        WHERE articul = %(article)s AND query = %(query)s
        ORDER BY created_at DESC
        LIMIT 1
        """
        
        try:
            self.logger.debug(f"[ParserWB] Поиск товара {article} в БД по запросу '{query}'")
            result = self._execute_query(query_sql, {'article': article, 'query': query})
            
            if result:
                position, name, brand, price, feedbacks, rating, promo_text, timestamp = result[0]
                self.logger.info(f"[ParserWB] Товар {article} найден в БД на позиции {position}")
                return {
                    'position': position,
                    'product_data': {
                        'name': name,
                        'brand': brand,
                        'price': price,
                        'feedbacks': feedbacks,
                        'rating': rating,
                        'promo_text': promo_text,
                        'last_update': timestamp
                    }
                }
            self.logger.warning(f"[ParserWB] Товар {article} не найден в БД")
            return None
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка при поиске товара в БД: {str(e)}", exc_info=True)
            return None

    def get_product_history(self, article: int, query: str, days: int = 7) -> List[Dict]:
        """
        Возвращает историю позиций товара за указанное количество дней
        """
        query_sql = """
        SELECT 
            if (promotion = 'no promotion' or promotion = '',position,promoPosition) as position_with_promo,
            price,
            number_of_feedbacks,
            created_at  
        FROM wildberries.data
        WHERE articul = %(article)s  AND query = %(query)s
          AND created_at <= %(date_from)s
        ORDER BY created_at
        """
        
        date_from = datetime.now()
        self.logger.info(f"[ParserWB] Получение истории товара {article} за последние {days} дней")
        
        try:
            results = self._execute_query(
                query_sql, 
                {
                    'article': article,
                    'query': query,
                    'date_from': date_from
                }
            )
            
            history = []
            for row in results:
                position, price, feedbacks, timestamp = row
                history.append({
                    'date': timestamp,
                    'position': position,
                    'price': price,
                    'feedbacks': feedbacks
                })
            
            self.logger.info(f"[ParserWB] Получено {len(history)} записей истории для товара {article}")
            return history
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка при получении истории товара: {str(e)}", exc_info=True)
            return []

    def _extract_product_data(self, product) -> Dict:
        """Извлекает основные данные о товаре"""
        try:
            price = product.sizes[0].get("price", {}).get("total", 0) // 100 if product.sizes else 0
            logistics = product.sizes[0].get("price", {}).get("logistics", 0) if product.sizes else 0
            
            data = {
                'id': product.id,
                'name': product.name if product.name else 'no name',
                'brand': product.brand if product.brand else 'no brand',
                'price': price,
                'logistics_cost': logistics,
                'rating': product.reviewRating if product.reviewRating else 0,
                'feedbacks': product.feedbacks if product.feedbacks else 0,
                'quantity': product.totalQuantity if product.totalQuantity else 0,
                'promo_text': product.promoTextCard if product.promoTextCard else 'no promo',
                'position': product.log.get("position") if product.log else -1,
                'promo_position': product.log.get("promoPosition") if product.log else -1,
                'colors': len(product.colors) if product.colors else 1
            }
            
            self.logger.debug(f"[ParserWB] Извлечены данные товара {product.id}: {data}")
            return data
            
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка при извлечении данных товара: {str(e)}", exc_info=True)
            return {
                'id': product.id,
                'name': 'error',
                'brand': 'error',
                'price': 0,
                'logistics_cost': 0,
                'rating': 0,
                'feedbacks': 0,
                'quantity': 0,
                'promo_text': 'error',
                'position': -1,
                'promo_position': -1,
                'colors': 0
            }
    
    def __save_to_db(self, items: Items):
        current_time = datetime.now()
        self.logger.debug(f"[ParserWB] Проверка времени последней записи для запроса '{self.query}'")
        
        if self.query in LAST_WRITE_TIMES:
            last_write_time = LAST_WRITE_TIMES[self.query]
            time_diff = (current_time - last_write_time).total_seconds()
            
            if time_diff < 3598:
                self.logger.warning(
                    f"[ParserWB] Данные по запросу '{self.query}' уже сохранялись {time_diff:.0f} секунд назад. "
                    "Пропускаем сохранение."
                )
                return False
        else:
            self.logger.info(f"[ParserWB] Первая запись для запроса '{self.query}'")
        
        data_to_insert = []
        self.logger.info(f"[ParserWB] Подготовка {len(items.products)} товаров для сохранения в БД")
        
        for i, product in enumerate(items.products): 
            price = product.sizes[0].get("price", {}).get("total", 0) // 100
            logistics = product.sizes[0].get("price", {}).get("logistics", 0)

            data_to_insert.append((
                product.id,
                self.query,
                datetime.now(),
                product.name if product.name else 'no name',
                product.brand if product.brand else 'no brand',
                price,
                logistics,
                product.reviewRating if product.reviewRating else 0,
                product.feedbacks if product.feedbacks else 0,
                product.totalQuantity if product.totalQuantity else 0,
                product.viewFlags if product.viewFlags else 0,
                product.pics if product.pics else 0,
                product.supplierFlags if product.supplierFlags else 0,
                product.supplierRating if product.supplierRating else 0,
                product.dist if product.dist else 0,
                str(product.log.get("promotion")) if product.log else "no promotion",
                product.log.get("tp") if product.log else "no tp",
                product.promoTextCard if product.promoTextCard else 'no promo',
                product.log.get("cpm") if product.log else 0,
                product.log.get("promoPosition") if product.log else -1,
                product.log.get("position") if product.log else i+1,
                len(product.colors) if product.colors else 0
            ))
        
        try:
            self.logger.info(f"[ParserWB] Начало вставки {len(data_to_insert)} записей в ClickHouse")
            self.client.execute(
                """INSERT INTO wildberries.data VALUES""",
                data_to_insert,
                types_check=True
            )
            
            LAST_WRITE_TIMES[self.query] = current_time
            self.logger.info(f"[ParserWB] Успешно сохранено {len(data_to_insert)} записей для запроса '{self.query}'")
            return True
            
        except Exception as e:
            self.logger.error(f"[ParserWB] Ошибка при вставке данных: {str(e)}", exc_info=True)
            return False

if __name__ == "__main__":
    # Настройка логирования при запуске напрямую (для тестирования)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bot.log'),
            logging.StreamHandler()
        ]
    )
    query = "шарф"
    parser = ParserWB(query)
    parser.logger.info(f"[ParserWB] Запуск парсера для запроса: '{query}'")