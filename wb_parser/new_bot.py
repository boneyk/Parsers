import telebot
from telebot.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
import schedule
import threading
import time
from datetime import datetime, timedelta
from parser import ParserWB  
import matplotlib
matplotlib.use('Agg')  
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import io


class WBTrackerBot:
    def __init__(self, token, superset_dashboard_url):
        self.bot = telebot.TeleBot(token)
        # Новая структура: {chat_id: {article: {query: {data}}}
        self.tracked_items = {}  
        self.superset_dashboard_url = superset_dashboard_url
        self.setup_handlers()
        
        self.scheduler_thread = threading.Thread(target=self.run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()

    def setup_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def start(message):
            self.show_main_menu(message.chat.id)

        @self.bot.message_handler(func=lambda m: m.text == '📊 Добавить товар')
        def add_product(message):
            self.bot.send_message(message.chat.id, "Напишите запрос, в котором будет отслеживаться положение товара (например, 'шарф'):")
            self.bot.register_next_step_handler(message, self.process_query)

        @self.bot.message_handler(func=lambda m: m.text == '📉 История отслеживания')
        def show_history(message):
            self.handle_history_request(message.chat.id)
        
        @self.bot.message_handler(func=lambda m: m.text == '❌ Удалить отслеживание')
        def remove_tracking(message):
            self.handle_remove_tracking_request(message.chat.id)

        @self.bot.message_handler(func=lambda m: m.text == '📈 Аналитика')
        def show_analytics(message):
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton(text="Открыть аналитику 📊", url=self.superset_dashboard_url))
            self.bot.send_message(message.chat.id, "Нажмите кнопку ниже, чтобы открыть полную аналитику:", reply_markup=markup)
    
    def show_main_menu(self, chat_id):
        markup = ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add('📊 Добавить товар', '📈 Аналитика')
        markup.add('❌ Удалить отслеживание','📉 История отслеживания')
        self.bot.send_message(chat_id, "Выберите действие:", reply_markup=markup)

    def handle_history_request(self, chat_id):
        """Обрабатывает запрос на просмотр истории"""
        if chat_id not in self.tracked_items or not self.tracked_items[chat_id]:
            self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
            return
        
        markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        
        for article in self.tracked_items[chat_id]:
            if isinstance(article, int): 
                queries = self.tracked_items[chat_id][article]
                for query, data in queries.items():
                    markup.add(f"История: {article} ({query})")
        
        markup.add("↩️ Назад")
        self.bot.send_message(
            chat_id, 
            "Выберите товар для просмотра истории:\n(Указан артикул и запрос)",
            reply_markup=markup
        )
        self.bot.register_next_step_handler_by_chat_id(chat_id, self.process_history_selection)

    def process_history_selection(self, message):
        """Обрабатывает выбор товара для просмотра истории"""
        if message.text == "↩️ Назад":
            self.show_main_menu(message.chat.id)
            return
        
        try:
            parts = message.text.split('(')
            article = int(parts[0].split(':')[1].strip())
            query = parts[1].replace(')', '').strip()
            
            chat_id = message.chat.id
            
            if (chat_id in self.tracked_items and 
                article in self.tracked_items[chat_id] and 
                query in self.tracked_items[chat_id][article]):
                self.send_product_history(chat_id, article, query)
            else:
                self.bot.send_message(chat_id, "Товар не найден в вашем списке отслеживания.")
        
        except Exception as e:
            print(f"Ошибка при обработке выбора: {e}")
            self.bot.send_message(
                message.chat.id, 
                "Некорректный формат. Используйте кнопки для выбора."
            )
            self.handle_history_request(message.chat.id)

    def send_product_history(self, chat_id, article, query):
        """Отправляет историю позиций товара с графиком за последние 7 дней"""
        try:
            parser = ParserWB(query)
            history = parser.get_product_history(article, query)
            
            if not history:
                self.bot.send_message(chat_id, f"Для товара {article} нет данных истории.")
                self.show_main_menu(chat_id)
                return
            
            days_ago = datetime.now() - timedelta(days=7)
            recent_history = [entry for entry in history if entry['date'] >= days_ago]
            
            if not recent_history:
                self.bot.send_message(chat_id, f"Для товара {article} нет данных за последние 7 дней.")
                self.show_main_menu(chat_id)
                return
                
            history_sorted = sorted(recent_history, key=lambda x: x['date'])
            dates = [entry['date'] for entry in history_sorted]
            positions = [entry['position'] for entry in history_sorted]
            
            plt.figure(figsize=(12, 6))            
            plt.plot(dates, positions, 'b-', marker='o', linewidth=2, markersize=8)
            
            plt.gca().invert_yaxis()
            plt.title(f'Динамика позиции товара {article}\nпо запросу "{query}"', pad=20)
            plt.xlabel('Дата')
            plt.ylabel('Позиция в каталоге')
            plt.grid(True, linestyle='--', alpha=0.3)
            
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d.%m.%y'))
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))
            plt.xticks(rotation=45, ha='right', fontsize=8)
            
            if len(positions) > 0:
                min_pos = min(positions)
                max_pos = max(positions)
                min_idx = positions.index(min_pos)
                max_idx = positions.index(max_pos)
                plt.annotate(f'Лучшая: {min_pos}',
                            xy=(dates[min_idx], min_pos),
                            xytext=(10, 20), textcoords='offset points',
                            bbox=dict(boxstyle='round,pad=0.5', fc='lime', alpha=0.7),
                            arrowprops=dict(arrowstyle='->'))
                plt.annotate(f'Худшая: {max_pos}',
                            xy=(dates[max_idx], max_pos),
                            xytext=(10, -30), textcoords='offset points',
                            bbox=dict(boxstyle='round,pad=0.5', fc='red', alpha=0.3),
                            arrowprops=dict(arrowstyle='->'))
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
            buf.seek(0)
            plt.close()
    
            history_text = f"📊 История позиций товара {article} по запросу '{query}':\n\n"
            
            current_date = None
            for entry in history_sorted:
                entry_date = entry['date'].strftime('%d.%m.%Y')
                if entry_date != current_date:
                    history_text += f"\n📅 {entry_date}:\n"
                    current_date = entry_date
                
                history_text += (
                    f"🕒 {entry['date'].strftime('%H:%M')}: "
                    f"Позиция {entry['position']} | "
                    f"Цена: {entry['price']} руб | "
                    f"Отзывы: {entry['feedbacks']}\n"
                )
            
            self.bot.send_message(chat_id, history_text[:4000])
            self.bot.send_photo(chat_id, photo=buf, 
                            caption=f'📈 График изменения позиции товара {article}\n'
                                    f'Запрос: "{query}" (7 дней)')
            
            self.show_main_menu(chat_id)
            
        except Exception as e:
            print(f"Ошибка при получении истории: {str(e)}")
            self.bot.send_message(chat_id, f"Произошла ошибка при построении графика для товара {article}.")
            self.show_main_menu(chat_id)
    
    def handle_remove_tracking_request(self, chat_id):
        """Обрабатывает запрос на удаление отслеживания"""
        if chat_id not in self.tracked_items or not self.tracked_items[chat_id]:
            self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
            return
        
        has_items_to_remove = any(
            isinstance(key, int) for key in self.tracked_items[chat_id].keys()
        )
    
        if not has_items_to_remove:
            self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
            return
        
        markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        
        for key, value in self.tracked_items[chat_id].items():
            if isinstance(key, int):  # Это артикул товара
                article = key
                for query in value.keys():
                    markup.add(f"Удалить: {article} ({query})")
            
        markup.add("↩️ Назад")
        self.bot.send_message(
            chat_id, 
            "Выберите товар для удаления отслеживания:\n(Указан артикул и запрос)",
            reply_markup=markup
        )
        self.bot.register_next_step_handler_by_chat_id(chat_id, self.process_remove_selection)

    def process_remove_selection(self, message):
        """Обрабатывает выбор товара для удаления"""
        if message.text == "↩️ Назад":
            self.show_main_menu(message.chat.id)
            return
        
        try:
            parts = message.text.split('(')
            article = int(parts[0].split(':')[1].strip())
            query = parts[1].replace(')', '').strip()
            
            chat_id = message.chat.id
            
            if (chat_id in self.tracked_items and 
                article in self.tracked_items[chat_id] and 
                query in self.tracked_items[chat_id][article]):
                
                job_tag = f'track_{chat_id}_{article}_{query}'
                schedule.clear(job_tag)
                
                del self.tracked_items[chat_id][article][query]
                
                if not self.tracked_items[chat_id][article]:
                    del self.tracked_items[chat_id][article]
                
                self.bot.send_message(chat_id, f"✅ Отслеживание товара {article} по запросу '{query}' удалено.")
            else:
                self.bot.send_message(chat_id, "Товар не найден в вашем списке отслеживания.")
        
        except Exception as e:
            print(f"Ошибка при обработке удаления: {e}")
            self.bot.send_message(
                message.chat.id, 
                "Некорректный формат. Используйте кнопки для выбора."
            )
        
        self.show_main_menu(message.chat.id)

    def process_query(self, message):
        query = message.text.lower()
        chat_id = message.chat.id
        
        if query.isdigit():
            self.bot.send_message(
                chat_id,
                "❌ Вы ввели число (похоже на артикул товара).\n"
                "Пожалуйста, введите текстовый поисковый запрос (например, 'шарфы', 'зимние куртки')."
            )
            return
        
        if len(query) < 2:
            self.bot.send_message(chat_id, "❌ Слишком короткий запрос. Введите минимум 2 символа.")
            return

        if chat_id not in self.tracked_items:
            self.tracked_items[chat_id] = {}
        
        self.tracked_items[chat_id]['current_query'] = query
        
        self.bot.send_message(chat_id, "Теперь отправьте артикул товара WB, который вы хотите отслеживать:")
        self.bot.register_next_step_handler(message, lambda m: self.process_article(m, query))

    def process_article(self, message, query):
        try:
            article = int(message.text)
            chat_id = message.chat.id
            
            if chat_id not in self.tracked_items:
                self.tracked_items[chat_id] = {}
            
            self.bot.send_message(chat_id, "Укажите периодичность проверок в день (например, 4):")
            self.bot.register_next_step_handler(
                message, 
                lambda m: self.process_frequency(m, query, article)
            )
            
        except ValueError:
            self.bot.send_message(message.chat.id, "Некорректный артикул. Попробуйте снова.")

    def process_frequency(self, message, query, article):
        try:
            frequency = int(message.text)
            chat_id = message.chat.id
            
            if frequency < 1 or frequency > 24:
                raise ValueError("Частота должна быть от 1 до 24 раз в день")
            
            search_msg = self.bot.send_message(
                chat_id, 
                f"🔍 Идет поиск товара в каталоге по запросу '{query}'..."
            )
            
            if chat_id not in self.tracked_items:
                self.tracked_items[chat_id] = {}
            if article not in self.tracked_items[chat_id]:
                self.tracked_items[chat_id][article] = {}
            
            self.tracked_items[chat_id][article][query] = {
                'frequency': frequency,
                'last_position': None
            }
            
            self.check_product_position(chat_id, article, query)
            
            self.bot.delete_message(chat_id, search_msg.message_id)
            self.setup_schedule(chat_id, article, query, frequency)
            
            self.bot.send_message(
                chat_id,
                f"✅ Товар {article} добавлен в отслеживание!\n"
                f"Запрос: '{query}'\n"
                f"Проверка: {frequency} раз(а) в день"
            )
            
        except ValueError as e:
            self.bot.send_message(message.chat.id, f"Ошибка: {str(e)}. Попробуйте снова.")
        except Exception as e:
            self.bot.send_message(message.chat.id, f"Произошла ошибка: {str(e)}")

    def check_product_position(self, chat_id, article, query):
        """Проверяет текущую позицию товара"""
        try:
            parser = ParserWB(query)
            result = parser.find_product_position(article, query)
            
            if not result:
                self.bot.send_message(chat_id, f"❌ Товар {article} не найден по запросу '{query}'")
                return
            
            current_position = result['position']
            product_data = result.get('product_data', {})
            last_position = self.tracked_items[chat_id][article][query].get('last_position')
            
            if last_position is None:
                self.bot.send_message(
                    chat_id,
                    f"🔍 Товар {article} по запросу '{query}':\n"
                    f"Текущая позиция: {current_position}\n"
                    f"Название: {product_data.get('name', 'Неизвестно')}\n"
                    f"Цена: {product_data.get('price', 'Неизвестно')} руб"
                )
            elif current_position != last_position:
                change = last_position - current_position
                arrow = "⬆️" if change > 0 else "⬇️"
                self.bot.send_message(
                    chat_id,
                    f"🔄 Изменение позиции товара {article} по запросу '{query}':\n"
                    f"Было: {last_position} → Стало: {current_position} {arrow}\n"
                    f"Изменение: {abs(change)} позиций\n"
                    f"Название: {product_data.get('name', 'Неизвестно')}\n"
                    f"Цена: {product_data.get('price', 'Неизвестно')} руб"
                )
            
            self.tracked_items[chat_id][article][query]['last_position'] = current_position
            
        except Exception as e:
            self.bot.send_message(chat_id, f"⚠️ Ошибка при проверке товара {article}: {str(e)}")

    def setup_schedule(self, chat_id, article, query, frequency_per_day):
        """Настраивает расписание проверок"""
        interval = max(1, 24 // frequency_per_day)
        
        job_tag = f'track_{chat_id}_{article}_{query}'
        schedule.clear(job_tag)
        
        schedule.every(interval).hours.do(
            self.check_product_position, 
            chat_id, article, query
        ).tag(job_tag)

    def run_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def run(self):
        try:
            self.bot.polling(none_stop=True, timeout=30)
        except Exception as e:
            print(f"Ошибка: {e}. Переподключение через 10 секунд...")
            time.sleep(10)
            self.run()


if __name__ == "__main__":
    SUPERSET_DASHBOARD_URL = "https://datalens.yandex.cloud/3imlj6hgfqdqp?_theme=dark&_lang=ru"
    TOKEN = "7603773242:AAGjg0RWeEjYPp__ySNJA9JswCMghi88z7A"
    bot = WBTrackerBot(
        token=TOKEN,
        superset_dashboard_url=SUPERSET_DASHBOARD_URL
    )
    bot.run()