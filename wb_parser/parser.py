import requests
import openpyxl
from model import Items

class ParserWB:
    def __init__(self, query: str):
        self.query = query
        self.filename = "wb_data.xlsx"

    def parse(self):
        _page = 1
        self.__create_excel()
        while True:
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
            headers = ["id", "название", "бренд", "цена (руб.)", "стоимость доставки", "рейтинг", "количество отзывов", "в наличии","количество просмотров","количество картинок","уровень продавца","рейтинг продавца","расстояние до товара","участвует в продвижении?","тип рекламы","рекламный слоган","ставка за тысячу показов", "место товара при продвижении","место товара без продвижения", "количество цветов"]
            sheet.append(headers)
        else:
            sheet = workbook[self.query]

        workbook.save(self.filename)

    def __save_excel(self, items: Items):
        """Сохраняет данные в нужный лист"""
        workbook = openpyxl.load_workbook(self.filename)
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

        workbook.save(self.filename)


if __name__ == "__main__":
    ParserWB("шарф").parse()
