import telebot
from telebot.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
import schedule
import threading
import time
from datetime import datetime
from parser import ParserWB


class WBTrackerBot:
    def __init__(self, token, superset_dashboard_url):
        self.bot = telebot.TeleBot(token)
        self.tracked_items = {}  # {chat_id: {article: {last_position: int, last_check: datetime}}}
        self.superset_dashboard_url = superset_dashboard_url  # Полный URL дашборда
        self.setup_handlers()
        
        # Запуск планировщика в отдельном потоке
        self.scheduler_thread = threading.Thread(target=self.run_scheduler)
        self.scheduler_thread.start()

    def setup_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def start(message):
            self.show_main_menu(message.chat.id)

        @self.bot.message_handler(func=lambda m: m.text == '📊 Добавить товар')
        def add_product(message):
            msg = self.bot.send_message(message.chat.id, "Отправьте артикул товара WB:")
            self.bot.register_next_step_handler(msg, self.process_article)

        @self.bot.message_handler(func=lambda m: m.text == '📈 Аналитика')
        def show_analytics(message):
            # Создаем кнопку с прямой ссылкой на дашборд
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton(
                    text="Открыть аналитику 📊", 
                    url=self.superset_dashboard_url
                )
            )
            self.bot.send_message(
                message.chat.id,
                "Нажмите кнопку ниже, чтобы открыть аналитику:",
                reply_markup=markup
            )

    def process_article(self, message):
        try:
            article = int(message.text)
            if message.chat.id not in self.tracked_items:
                self.tracked_items[message.chat.id] = {}
            
            self.tracked_items[message.chat.id][article] = {
                'last_position': None,
                'history': []
            }
            
            self.bot.send_message(message.chat.id, f"Товар {article} добавлен в отслеживание!")
        except ValueError:
            self.bot.send_message(message.chat.id, "Некорректный артикул. Попробуйте снова.")

    def show_main_menu(self, chat_id):
        markup = ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add('📊 Добавить товар', '📉 Мои товары')
        markup.add('⚙️ Настройки', '📈 Аналитика')
        self.bot.send_message(chat_id, "Выберите действие:", reply_markup=markup)

    def run_scheduler(self):
        schedule.every(12).hours.do(self.check_positions)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def check_positions(self):
        for chat_id, products in self.tracked_items.items():
            for article, data in products.items():
                current_position = self.get_current_position(article)
                
                if data['last_position'] and current_position != data['last_position']:
                    change = data['last_position'] - current_position
                    arrow = "⬆️" if change > 0 else "⬇️"
                    self.bot.send_message(
                        chat_id,
                        f"🔄 Изменение позиции товара {article}:\n"
                        f"Было: {data['last_position']} → Стало: {current_position} {arrow}\n"
                        f"Изменение: {abs(change)} позиций"
                    )
                
                # Анализ конкурентов и рекомендации
                analysis = self.analyze_competitors(article)
                self.send_recommendations(chat_id, article, analysis)
                
                # Обновляем данные
                data['last_position'] = current_position
                data['history'].append({
                    'date': datetime.now(),
                    'position': current_position
                })

    def get_current_position(self, article: int) -> int:
        query = self.get_saved_query_for_article(article)
        if not query:
            return None
            
        parser = ParserWB(query)
        result = parser.find_product_position(article, query)
        
        if result:
            # Можно сохранить дополнительные данные о товаре
            self.save_product_data(article, result['product_data'])
            return result['position']
        
        return None

    def analyze_competitors(self, article) -> dict:
        """Анализ похожих товаров"""
        # Ваш код для анализа конкурентов
        return {
            'avg_price': 2500,
            'top_positions': [12345, 67890],
            'cpm_values': [10.5, 12.3, 15.0]
        }

    def send_recommendations(self, chat_id, article, analysis):
        """Отправка рекомендаций по рекламе"""
        markup = telebot.types.InlineKeyboardMarkup()
        btn = telebot.types.InlineKeyboardButton(
            text="Редактировать ставку", 
            callback_data=f"edit_cpm_{article}")
        markup.add(btn)
        
        self.bot.send_message(
            chat_id,
            f"📊 Аналитика по товару {article}:\n"
            f"Средняя цена конкурентов: {analysis['avg_price']} руб\n"
            f"Рекомендуемая ставка CPM: {max(analysis['cpm_values']) * 1.2:.2f} руб\n\n"
            f"Для поднятия на 10 позиций увеличьте ставку на 15%",
            reply_markup=markup
        )

    def run(self):
        self.bot.polling(none_stop=True)

if __name__ == "__main__":
    # Полный URL вашего дашборда в Superset
    SUPERSET_DASHBOARD_URL = "https://i.pinimg.com/736x/a6/1e/1c/a61e1cad0c0970aabc9ce4f305589a5e.jpg"
    
    bot = WBTrackerBot(
        token="7603773242:AAGjg0RWeEjYPp__ySNJA9JswCMghi88z7A",
        superset_dashboard_url=SUPERSET_DASHBOARD_URL
    )
    bot.run()