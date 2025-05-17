import openpyxl
import csv
from model import Items
from datetime import datetime
from clickhouse_driver import Client
import requests
from typing import Optional, Dict, List, Tuple

LAST_WRITE_TIMES = {}

class ParserWB:
    def __init__(self, query: str):
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
            print("Соединение с ClickHouse установлено успешно")
        
        except Exception as e:
            print(f"Ошибка подключения к ClickHouse: {str(e)}")
            raise

    def _execute_query(self, query, params=None):
        """Выполняет SQL-запрос к ClickHouse через clickhouse-driver"""
        try:
            return self.client.execute(query, params)
        except Exception as e:
            print(f"Ошибка выполнения запроса: {str(e)}")
            return None

    def parse(self):
        _page = 1
        all_items = Items(products=[])

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

            response = requests.get('https://search.wb.ru/exactmatch/ru/common/v9/search', params=params)
            _page += 1
            
            if response.status_code == 200:
                data = response.json().get("data", {})
                
                if "products" in data and isinstance(data["products"], list) and data["products"]:
                    items_info = Items.model_validate(data)
                    all_items.products.extend(items_info.products)
                    print(f"Прочитаны данные со страницы: {_page-1}")
                else:
                    print("Список товаров пуст или отсутствует.")
            else:
                print(f"Ошибка при запросе данных! Код: {response.status_code}")
        
        if all_items.products:
            print(f"Загружаем {len(all_items.products)} записей в ClickHouse...")
            result = self.__save_to_db(all_items)
            if result:
                print(f"Успешно загружено {len(all_items.products)} записей")
            else:
                print("Ошибка при загрузке данных")
            return all_items

    def find_product_position(self, article: int, query: str) -> Optional[Dict]:
        """
        Ищет товар по артикулу в текущем запросе и возвращает его позицию и данные
        Возвращает словарь с ключами: position, product_data
        """
        try:
            parsed_data = self.parse()
            if parsed_data:
                for idx, product in enumerate(parsed_data.products):
                    if product.id == article:
                        return {
                            'position': idx + 1,
                            'product_data': self._extract_product_data(product)
                        }
            
            print("Не нашли в текущем парсинге, ищем в базе данных")
            return self._find_product_in_db(article, query)

            
        except Exception as e:
            print(f"Ошибка при поиске товара {article}: {str(e)}")
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
            result = self._execute_query(query_sql, {'article': article, 'query': query})
            if result:
                position, name, brand, price, feedbacks, rating, promo_text, timestamp = result[0]
                print("Товар найден и добавлен в отслеживание.")
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
            return None
        except Exception as e:
            print(f"Ошибка при поиске товара в БД: {str(e)}")
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
            
            return history
        except Exception as e:
            print(f"Ошибка при получении истории товара: {str(e)}")
            return []


    def _extract_product_data(self, product) -> Dict:
        """Извлекает основные данные о товаре"""
        price = product.sizes[0].get("price", {}).get("total", 0) // 100 if product.sizes else 0
        logistics = product.sizes[0].get("price", {}).get("logistics", 0) if product.sizes else 0
        
        return {
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
    
    def __save_to_db(self, items: Items):
    
        current_time = datetime.now()
        print(LAST_WRITE_TIMES)
        if self.query in LAST_WRITE_TIMES:
            last_write_time = LAST_WRITE_TIMES[self.query]
            print(f"Текущее время: {current_time}, Время последней записи: {last_write_time}")
            
            if (current_time - last_write_time).total_seconds() < 3598:
                print(f"Данные по запросу '{self.query}' уже сохранялись менее часа назад. Пропускаем.")
                return False
        else:
            print(f"Первая запись для запроса '{self.query}'")
        
        data_to_insert = []
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
            # Вставляем данные одной операцией
            self.client.execute(
                """INSERT INTO wildberries.data VALUES""",
                data_to_insert,types_check=True
            )
            LAST_WRITE_TIMES[self.query] = current_time
            print(LAST_WRITE_TIMES)
            return True
        except Exception as e:
            print(f"Ошибка при вставке данных: {str(e)}")
            return False

if __name__ == "__main__":
    quary = "шарф"
    print("Парсим данные...")
    # ParserWB(quary).parse()