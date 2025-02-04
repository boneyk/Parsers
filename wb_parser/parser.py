import requests
import openpyxl
from model import Items

class ParserWB:
    def __init__(self, query: str):
        self.query = query
        self.filename = "wb_data.xlsx"

    def parse(self):
        self.__create_excel()
        params = {
            'ab_testing': 'false',
            'appType': '1',
            'curr': 'rub',
            'dest': '123585528',
            'lang': 'ru',
            'page': '1',
            'query': self.query,
            'resultset': 'catalog',
            'sort': 'popular',
            'spp': '30',
            'suppressSpellcheck': 'false',
        }

        response = requests.get('https://search.wb.ru/exactmatch/ru/common/v9/search', params=params)

        if response.status_code == 200:
            data = response.json().get("data", {})

            if "products" in data and isinstance(data["products"], list) and data["products"]:
                items_info = Items.model_validate(data)  # Обрабатываем все товары
                self.__save_excel(items_info)
            else:
                print("Список товаров пуст или отсутствует.")
        else:
            print("Ошибка при запросе данных!")
            print(response.status_code)

    def __create_excel(self):
        """Создаёт или открывает Excel-файл и создаёт лист для запроса"""
        try:
            workbook = openpyxl.load_workbook(self.filename)
        except FileNotFoundError:
            workbook = openpyxl.Workbook()

        if self.query not in workbook.sheetnames:
            sheet = workbook.create_sheet(title=self.query)
            headers = ["id", "название", "бренд", "цена (руб.)", "рейтинг", "количество отзывов", "в наличии"]
            sheet.append(headers)
        else:
            sheet = workbook[self.query]

        workbook.save(self.filename)

    def __save_excel(self, items: Items):
        """Сохраняет данные в нужный лист"""
        workbook = openpyxl.load_workbook(self.filename)
        sheet = workbook[self.query]

        for product in items.products:
            price = product.sizes[0].get("price", {}).get("total")
            price = price // 100
            sheet.append([
                product.id,
                product.name,
                product.brand,
                price,
                product.reviewRating,
                product.feedbacks,
                product.volume
            ])

        workbook.save(self.filename)


if __name__ == "__main__":
    ParserWB("шарф").parse()
