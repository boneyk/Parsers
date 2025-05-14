import openpyxl
import csv
from model import Items
from datetime import datetime
from clickhouse_driver import Client
import requests

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
            
            # Тестовый запрос для проверки соединения
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
        
        # Проверяем структуру таблицы
        # structure = self.client.execute("DESCRIBE TABLE wildberries.data")
        # print("Структура таблицы:")
        # for column in structure:
        #     print(column)

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

    def __save_to_db(self, items: Items):
        if not items or not items.products:
            print("Нет данных для сохранения")
            return False
        
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
                product.log.get("position") if product.log else i,
                len(product.colors) if product.colors else 0
            ))
        try:
            # Вставляем данные одной операцией
            self.client.execute(
                """INSERT INTO wildberries.data VALUES""",
                data_to_insert,types_check=True
            )
            return True
        except Exception as e:
            print(f"Ошибка при вставке данных: {str(e)}")
            return False
        #     data_to_insert.append((
        #         product.id,
        #         self.query,
        #         datetime.now(),
        #         product.name,
        #         product.brand or "no brand",
        #         price,
        #         logistics,
        #         product.reviewRating,
        #         product.feedbacks,
        #         product.totalQuantity,
        #         product.viewFlags,
        #         product.pics,
        #         product.supplierFlags,
        #         product.supplierRating,
        #         product.dist or 0,
        #         promotion,
        #         tp,
        #         product.promoTextCard or "no promoText",
        #         cpm,
        #         promoPosition,
        #         position,
        #         count_of_colors
        #     ))

        # insert_query = f"""
        # INSERT INTO wildberries.data VALUES {data_to_insert}
        # """
        # return self._execute_query(insert_query)

    
    def __create_excel(self):
        try:
            workbook = openpyxl.load_workbook(self.filename_excel)
        except FileNotFoundError:
            workbook = openpyxl.Workbook()

        if self.query not in workbook.sheetnames:
            sheet = workbook.create_sheet(title=self.query)
            headers = ["id","время создания", "название", "бренд", "цена", "стоимость_доставки", "рейтинг", "количество_отзывов", "в_наличии","количество_просмотров","количество_картинок","уровень_продавца","рейтинг_продавца","расстояние_до_товара","участвует_в_продвижении?","тип_рекламы","рекламный_слоган","рекламная ставка", "промо_место","место_без_продвижения", "количество_цветов","запрос"]
            sheet.append(headers)
        else:
            sheet = workbook[self.query]

        workbook.save(self.filename_excel)

    def __save_excel(self, items: Items):
        workbook = openpyxl.load_workbook(self.filename_excel)
        sheet = workbook[self.query]

        for i, product in enumerate(items.products):
            price = product.sizes[0].get("price", {}).get("total")
            logistics = product.sizes[0].get("price", {}).get("logistics")

            promotion = None
            cpm = None
            promoPosition = None
            position = i
            tp = None
            count_of_colors = None

            if product.log is not None:
                promotion = product.log.get("promotion")
                cpm = product.log.get("cpm")
                promoPosition = product.log.get("promoPosition")
                position = product.log.get("position")
                tp = product.log.get("tp")
            if product.colors is not None:
                count_of_colors = len(product.colors)

            # print(product)
            price = price // 100
            sheet.append([
                product.id,
                (datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                product.name,
                product.brand,
                price,
                logistics,
                product.reviewRating,
                product.feedbacks,
                product.totalQuantity,
                product.viewFlags,
                product.pics,
                product.supplierFlags,
                product.supplierRating,
                product.dist,
                promotion,
                tp,
                product.promoTextCard,
                cpm,
                promoPosition,
                position,
                count_of_colors
            ])
        workbook.save(self.filename_excel)
        print(f"В Excel записано товаров")

    def __save_csv(self, items: Items):
        with open(self.filename_csv, mode='a', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            for i, product in enumerate(items.products):
                price = product.sizes[0].get("price", {}).get("total")
                logistics = product.sizes[0].get("price", {}).get("logistics")

                promotion = None
                cpm = None
                promoPosition = None
                position = i
                tp = None
                count_of_colors = None

                if product.log is not None:
                    promotion = product.log.get("promotion")
                    cpm = product.log.get("cpm")
                    promoPosition = product.log.get("promoPosition")
                    position = product.log.get("position")
                    tp = product.log.get("tp")
                if product.colors is not None:
                    count_of_colors = len(product.colors)

                price = price // 100 if price is not None else None
                csv_writer.writerow([
                    product.id,
                    (datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
                    product.name,
                    product.brand,
                    price,
                    logistics,
                    product.reviewRating,
                    product.feedbacks,
                    product.totalQuantity,
                    product.viewFlags,
                    product.pics,
                    product.supplierFlags,
                    product.supplierRating,
                    product.dist,
                    promotion,
                    tp,
                    product.promoTextCard,
                    cpm,
                    promoPosition,
                    position,
                    count_of_colors,
                    self.query
                ])
            print(f"В CSV записаны товары")


if __name__ == "__main__":
    quary = "шарф"
    print("Парсим данные...")
    ParserWB(quary).parse()