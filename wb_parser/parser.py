import requests
import openpyxl
import csv
from model import Items
from datetime import datetime
from clickhouse_driver import Client

class ParserWB:
    def __init__(self, query: str):
        self.query = query
        self.filename_excel = f"{query}.xlsx"
        self.filename_csv = f"{query}.csv"

        # Если контейнер в сети Docker: "clickhouse"
        # self.client = Client(host="clickhouse:")  
        # self.__create_table()

    def parse(self):
        _page = 1
        self.__create_excel()
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
                time = datetime.now()

                if "products" in data and isinstance(data["products"], list) and data["products"]:
                    items_info = Items.model_validate(data)  # Обрабатываем все товары
                    # self.__save_excel(items_info)
                    self.__save_csv(items_info)
                    # self.__save_to_db(items_info)
                    print(f"Добавлены данные со старницы: {_page-1}")

                else:
                    print("Список товаров пуст или отсутствует.")
            else:
                print("Ошибка при запросе данных!")
                print(response.status_code)

    def __create_table(self):
        """Создаёт таблицу в ClickHouse, если её нет"""
        self.client.execute("""
        CREATE TABLE IF NOT EXISTS wildberries (
            user_id UInt64,
            created_at DateTime,
            name String,
            brand String,
            price Float64,
            logistics Float64,
            reviewRating Float64,
            number_of_feedbacks UInt32,
            totalQuantity UInt32,
            viewFlags UInt32,
            pics UInt32,
            supplierFlags UInt32,
            supplierRating Float64,
            dist UInt32,
            promotion String,
            tp String,
            promoTextCard String,
            cpm Float64,
            promoPosition UInt32,
            position UInt32,
            count_of_colors UInt32
        ) ENGINE = MergeTree()
        ORDER BY id
        """)
        print("Таблица wildberries в ClickHouse создана")

    def __save_to_db(self, items: Items):
        data_to_insert = []
        for i, product in enumerate(items.products):
            price = product.sizes[0].get("price", {}).get("total", 0) // 100
            logistics = product.sizes[0].get("price", {}).get("logistics", 0)

            promotion = product.log.get("promotion") if product.log else ""
            cpm = product.log.get("cpm") if product.log else 0
            promoPosition = product.log.get("promoPosition") if product.log else 0
            position = product.log.get("position") if product.log else i
            tp = product.log.get("tp") if product.log else ""
            count_of_colors = len(product.colors) if product.colors else 0

            data_to_insert.append((
                product.id,
                datetime.now(),
                product.name,
                product.brand or "",
                price,
                logistics,
                product.reviewRating,
                product.feedbacks,
                product.totalQuantity,
                product.viewFlags,
                product.pics,
                product.supplierFlags,
                product.supplierRating,
                product.dist or 0,
                promotion,
                tp,
                product.promoTextCard or "",
                cpm,
                promoPosition,
                position,
                count_of_colors
            ))

        self.client.execute("""
            INSERT INTO wildberries VALUES
            """, data_to_insert)
        print(f"В ClickHouse записано {len(data_to_insert)} товаров")
    
    def __create_excel(self):
        try:
            workbook = openpyxl.load_workbook(self.filename_excel)
        except FileNotFoundError:
            workbook = openpyxl.Workbook()

        if self.query not in workbook.sheetnames:
            sheet = workbook.create_sheet(title=self.query)
            headers = ["id","время создания", "название", "бренд", "цена", "стоимость_доставки", "рейтинг", "количество_отзывов", "в_наличии","количество_просмотров","количество_картинок","уровень_продавца","рейтинг_продавца","расстояние_до_товара","участвует_в_продвижении?","тип_рекламы","рекламный_слоган","рекламная ставка", "промо_место","место_без_продвижения", "количество_цветов"]
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

            print(product)
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
                    count_of_colors
                ])
            print(f"В CSV записаны товары")



if __name__ == "__main__":
    quary = "шарф"
    ParserWB(quary).parse()