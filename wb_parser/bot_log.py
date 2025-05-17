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
import logging
import os


class WBTrackerBot:
    def __init__(self, token, superset_dashboard_url):
        self.setup_logging()
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Инициализация бота...")
        
        self.bot = telebot.TeleBot(token)
        # структура: {chat_id: {article: {query: {data}}}
        self.tracked_items = {}  
        self.superset_dashboard_url = superset_dashboard_url
        self.setup_handlers()
        
        self.scheduler_thread = threading.Thread(target=self.run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        self.logger.info("Бот инициализирован")

    def setup_logging(self):
        """Настройка системы логирования"""
        log_file = 'bot.log'
        
        # Очищаем файл лога при каждом запуске
        if os.path.exists(log_file):
            with open(log_file, 'w',encoding='utf-8'):
                pass
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def setup_handlers(self):
        @self.bot.message_handler(commands=['start'])
        def start(message):
            self.logger.info(f"Обработка команды /start от пользователя {message.chat.id}")
            self.show_main_menu(message.chat.id)

        @self.bot.message_handler(func=lambda m: m.text == '📊 Добавить товар')
        def add_product(message):
            self.logger.info(f"Пользователь {message.chat.id} начал добавление товара")
            self.bot.send_message(message.chat.id, "Напишите запрос, в котором будет отслеживаться положение товара (например, 'шарф'):")
            self.bot.register_next_step_handler(message, self.process_query)

        @self.bot.message_handler(func=lambda m: m.text == '📉 История отслеживания')
        def show_history(message):
            self.logger.info(f"Пользователь {message.chat.id} запросил историю отслеживания")
            self.handle_history_request(message.chat.id)
        
        @self.bot.message_handler(func=lambda m: m.text == '❌ Удалить отслеживание')
        def remove_tracking(message):
            self.logger.info(f"Пользователь {message.chat.id} запросил удаление отслеживания")
            self.handle_remove_tracking_request(message.chat.id)

        @self.bot.message_handler(func=lambda m: m.text == '📈 Аналитика')
        def show_analytics(message):
            self.logger.info(f"Пользователь {message.chat.id} запросил аналитику")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton(text="Открыть аналитику 📊", url=self.superset_dashboard_url))
            self.bot.send_message(message.chat.id, "Нажмите кнопку ниже, чтобы открыть полную аналитику:", reply_markup=markup)
    
    def show_main_menu(self, chat_id):
        try:
            markup = ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add('📊 Добавить товар', '📈 Аналитика')
            markup.add('❌ Удалить отслеживание','📉 История отслеживания')
            self.bot.send_message(chat_id, "Выберите действие:", reply_markup=markup)
            self.logger.info(f"Отображено главное меню для пользователя {chat_id}")
        except Exception as e:
            self.logger.error(f"Ошибка при отображении главного меню: {str(e)}", exc_info=True)
            raise

    def handle_history_request(self, chat_id):
        """Обрабатывает запрос на просмотр истории"""
        try:
            if chat_id not in self.tracked_items or not self.tracked_items[chat_id]:
                self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
                self.logger.info(f"Пользователь {chat_id} не имеет отслеживаемых товаров")
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
            self.logger.info(f"Пользователю {chat_id} показаны варианты истории")
        except Exception as e:
            self.logger.error(f"Ошибка при обработке запроса истории: {str(e)}", exc_info=True)
            self.bot.send_message(chat_id, "Произошла ошибка при обработке запроса.")

    def process_history_selection(self, message):
        """Обрабатывает выбор товара для просмотра истории"""
        try:
            if message.text == "↩️ Назад":
                self.show_main_menu(message.chat.id)
                return
            
            self.logger.info(f"Пользователь {message.chat.id} выбрал: {message.text}")
            
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
                self.logger.warning(f"Товар {article} не найден для пользователя {chat_id}")
        
        except Exception as e:
            self.logger.error(f"Ошибка при обработке выбора истории: {str(e)}", exc_info=True)
            self.bot.send_message(
                message.chat.id, 
                "Некорректный формат. Используйте кнопки для выбора."
            )
            self.handle_history_request(message.chat.id)

    def send_product_history(self, chat_id, article, query):
        """Отправляет историю позиций товара с графиком за последние 7 дней"""
        try:
            self.logger.info(f"Формирование истории для товара {article} по запросу '{query}'")
            
            parser = ParserWB(query)
            history = parser.get_product_history(article, query)
            
            if not history:
                self.bot.send_message(chat_id, f"Для товара {article} нет данных истории.")
                self.show_main_menu(chat_id)
                self.logger.warning(f"Нет данных истории для товара {article}")
                return
            
            days_ago = datetime.now() - timedelta(days=7)
            recent_history = [entry for entry in history if entry['date'] >= days_ago]
            
            if not recent_history:
                self.bot.send_message(chat_id, f"Для товара {article} нет данных за последние 7 дней.")
                self.show_main_menu(chat_id)
                self.logger.warning(f"Нет данных за 7 дней для товара {article}")
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
            self.logger.info(f"История для товара {article} успешно отправлена пользователю {chat_id}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при получении истории: {str(e)}", exc_info=True)
            self.bot.send_message(chat_id, f"Произошла ошибка при построении графика для товара {article}.")
            self.show_main_menu(chat_id)
    
    def handle_remove_tracking_request(self, chat_id):
        """Обрабатывает запрос на удаление отслеживания"""
        try:
            if chat_id not in self.tracked_items or not self.tracked_items[chat_id]:
                self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
                self.logger.info(f"Пользователь {chat_id} не имеет товаров для удаления")
                return
            
            has_items_to_remove = any(
                isinstance(key, int) for key in self.tracked_items[chat_id].keys()
            )
        
            if not has_items_to_remove:
                self.bot.send_message(chat_id, "У вас нет отслеживаемых товаров.")
                self.logger.info(f"Пользователь {chat_id} не имеет товаров для удаления")
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
            self.logger.info(f"Пользователю {chat_id} показаны варианты для удаления")
        except Exception as e:
            self.logger.error(f"Ошибка при обработке запроса на удаление: {str(e)}", exc_info=True)
            self.bot.send_message(chat_id, "Произошла ошибка при обработке запроса.")

    def process_remove_selection(self, message):
        """Обрабатывает выбор товара для удаления"""
        try:
            if message.text == "↩️ Назад":
                self.show_main_menu(message.chat.id)
                return
            
            self.logger.info(f"Пользователь {message.chat.id} выбрал удаление: {message.text}")
            
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
                self.logger.info(f"Удалено отслеживание товара {article} для пользователя {chat_id}")
            else:
                self.bot.send_message(chat_id, "Товар не найден в вашем списке отслеживания.")
                self.logger.warning(f"Товар {article} не найден для удаления у пользователя {chat_id}")
        
        except Exception as e:
            self.logger.error(f"Ошибка при обработке удаления: {str(e)}", exc_info=True)
            self.bot.send_message(
                message.chat.id, 
                "Некорректный формат. Используйте кнопки для выбора."
            )
        
        self.show_main_menu(message.chat.id)

    def process_query(self, message):
        try:
            query = message.text.lower()
            chat_id = message.chat.id
            
            self.logger.info(f"Пользователь {chat_id} ввел запрос: {query}")
            
            if query.isdigit():
                self.bot.send_message(
                    chat_id,
                    "❌ Вы ввели число (похоже на артикул товара).\n"
                    "Пожалуйста, введите текстовый поисковый запрос (например, 'шарфы', 'зимние куртки')."
                )
                self.logger.warning(f"Пользователь {chat_id} ввел число вместо запроса")
                return
            
            if len(query) < 2:
                self.bot.send_message(chat_id, "❌ Слишком короткий запрос. Введите минимум 2 символа.")
                self.logger.warning(f"Слишком короткий запрос от пользователя {chat_id}")
                return

            if chat_id not in self.tracked_items:
                self.tracked_items[chat_id] = {}
            
            self.tracked_items[chat_id]['current_query'] = query
            
            self.bot.send_message(chat_id, "Теперь отправьте артикул товара WB, который вы хотите отслеживать:")
            self.bot.register_next_step_handler(message, lambda m: self.process_article(m, query))
            self.logger.info(f"Запрос '{query}' принят, ожидаем артикул от пользователя {chat_id}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке запроса: {str(e)}", exc_info=True)
            self.bot.send_message(message.chat.id, "Произошла ошибка при обработке запроса.")

    def process_article(self, message, query):
        try:
            article = int(message.text)
            chat_id = message.chat.id
            
            self.logger.info(f"Пользователь {chat_id} ввел артикул: {article}")
            
            if chat_id not in self.tracked_items:
                self.tracked_items[chat_id] = {}
            
            self.bot.send_message(chat_id, "Укажите периодичность проверок в день (например, 4):")
            self.bot.register_next_step_handler(
                message, 
                lambda m: self.process_frequency(m, query, article)
            )
            self.logger.info(f"Артикул {article} принят, ожидаем частоту проверок от пользователя {chat_id}")
            
        except ValueError:
            self.logger.warning(f"Некорректный артикул от пользователя {message.chat.id}")
            self.bot.send_message(message.chat.id, "Некорректный артикул. Попробуйте снова.")
        except Exception as e:
            self.logger.error(f"Ошибка при обработке артикула: {str(e)}", exc_info=True)
            self.bot.send_message(message.chat.id, "Произошла ошибка при обработке артикула.")

    def process_frequency(self, message, query, article):
        try:
            frequency = int(message.text)
            chat_id = message.chat.id
            
            self.logger.info(f"Пользователь {chat_id} указал частоту проверок: {frequency}")
            
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
            
            self.logger.info(f"Товар {article} успешно добавлен в отслеживание для пользователя {chat_id}")
            
        except ValueError as e:
            self.logger.warning(f"Некорректная частота проверок от пользователя {message.chat.id}: {str(e)}")
            self.bot.send_message(message.chat.id, f"Ошибка: {str(e)}. Попробуйте снова.")
        except Exception as e:
            self.logger.error(f"Ошибка при добавлении товара: {str(e)}", exc_info=True)
            self.bot.send_message(message.chat.id, f"Произошла ошибка: {str(e)}")

    def check_product_position(self, chat_id, article, query):
        """Проверяет текущую позицию товара"""
        try:
            self.logger.info(f"Проверка позиции товара {article} по запросу '{query}' для пользователя {chat_id}")
            
            parser = ParserWB(query)
            result = parser.find_product_position(article, query)
            
            if not result:
                self.bot.send_message(chat_id, f"❌ Товар {article} не найден по запросу '{query}'")
                self.logger.warning(f"Товар {article} не найден по запросу '{query}'")
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
                self.logger.info(f"Первая проверка товара {article}: позиция {current_position}")
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
                self.logger.info(f"Изменение позиции товара {article}: {last_position} → {current_position}")
            
            self.tracked_items[chat_id][article][query]['last_position'] = current_position
            
        except Exception as e:
            self.logger.error(f"Ошибка при проверке товара {article}: {str(e)}", exc_info=True)
            self.bot.send_message(chat_id, f"⚠️ Ошибка при проверке товара {article}: {str(e)}")

    def setup_schedule(self, chat_id, article, query, frequency_per_day):
        """Настраивает расписание проверок"""
        try:
            interval = max(1, 24 // frequency_per_day)
            
            job_tag = f'track_{chat_id}_{article}_{query}'
            schedule.clear(job_tag)
            
            schedule.every(interval).hours.do(
                self.check_product_position, 
                chat_id, article, query
            ).tag(job_tag)
            
            self.logger.info(f"Настроено расписание для товара {article}: проверка каждые {interval} часов")
        except Exception as e:
            self.logger.error(f"Ошибка при настройке расписания: {str(e)}", exc_info=True)
            raise

    def run_scheduler(self):
        """Запуск планировщика задач"""
        self.logger.info("Запуск планировщика задач")
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Ошибка в планировщике: {str(e)}", exc_info=True)
                time.sleep(10)

    def run(self):
        """Основной цикл работы бота"""
        self.logger.info("Запуск бота")
        while True:
            try:
                self.bot.polling(none_stop=True, timeout=30)
            except Exception as e:
                self.logger.error(f"Ошибка в основном цикле бота: {e}. Переподключение через 10 секунд...", exc_info=True)
                time.sleep(10)


if __name__ == "__main__":
    DASHBOARD_URL = "https://datalens.yandex.cloud/3imlj6hgfqdqp?_theme=dark&_lang=ru"
    TOKEN = "7603773242:AAGjg0RWeEjYPp__ySNJA9JswCMghi88z7A"
    bot = WBTrackerBot(
        token=TOKEN,
        superset_dashboard_url=DASHBOARD_URL
    )
    bot.run()