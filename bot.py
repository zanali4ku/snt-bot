import asyncio
import sqlite3
import pandas as pd
import json
import os
from pathlib import Path
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import FSInputFile
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime, timedelta
from collections import defaultdict  # для админского экспорта

# Настройка локального хранилища
STORAGE_DIR = Path("local_storage")
STORAGE_DIR.mkdir(exist_ok=True)

# Состояния для FSM
class AdminEditStates(StatesGroup):
    # Состояния для редактирования показаний
    select_user_for_reading = State()
    select_reading = State()
    enter_new_reading_value = State()
    
    # Состояния для редактирования пользователя
    select_user_for_edit = State()
    select_user_field = State()
    enter_new_user_value = State()
    
    # Состояния для добавления пользователя
    add_user_full_name = State()
    add_user_plot_number = State()
    add_user_street = State()
    add_user_street_name = State()  # Новое состояние для ввода улицы
    
    # Состояния для удаления пользователя
    select_user_for_delete = State()
    confirm_delete_user = State()
    
    # Состояния для добавления показания админом
    add_reading_user_id = State()
    add_reading_value = State()

# Состояния для регистрации
class RegistrationStates(StatesGroup):
    waiting_for_full_name = State()
    waiting_for_street = State()
    waiting_for_plot_number = State()

# Класс для проверки администратора
class IsAdminFilter:
    def __init__(self):
        self.admin_ids = [318928095, 596121287, 398143219, 1433000858]  # Ваш ID администратора

    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in self.admin_ids

# Класс для работы с локальными данными
class LocalStorage:
    @staticmethod
    def save_reading(user_id: int, value: float):
        """Сохраняет показание локально в файл"""
        file_path = STORAGE_DIR / f"{user_id}.json"
        
        try:
            if file_path.exists():
                with open(file_path, 'r') as f:
                    data = json.load(f)
            else:
                data = []
                
            data.append({
                "value": value,
                "timestamp": datetime.now().isoformat()
            })
            
            with open(file_path, 'w') as f:
                json.dump(data, f)
                
            return True
        except Exception as e:
            print(f"Ошибка сохранения локальных данных: {e}")
            return False

    @staticmethod
    def get_readings(user_id: int):
        """Получает локально сохраненные показания"""
        file_path = STORAGE_DIR / f"{user_id}.json"
        
        if not file_path.exists():
            return []
            
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка чтения локальных данных: {e}")
            return []

    @staticmethod
    def clear_readings(user_id: int):
        """Очищает локальные показания после успешной синхронизации"""
        file_path = STORAGE_DIR / f"{user_id}.json"
        try:
            if file_path.exists():
                file_path.unlink()
            return True
        except Exception as e:
            print(f"Ошибка удаления локальных данных: {e}")
            return False

# Инициализация базы данных
class Database:
    def __init__(self):
        self.conn = sqlite3.connect('snt_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS users
                            (user_id INTEGER PRIMARY KEY,
                             full_name TEXT,
                             plot_number TEXT,
                             street TEXT)''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS readings
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                             user_id INTEGER,
                             value REAL,
                             date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                             FOREIGN KEY(user_id) REFERENCES users(user_id))''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS global_reminders
                            (id INTEGER PRIMARY KEY AUTOINCREMENT,
                             is_active BOOLEAN DEFAULT 1,
                             reminder_day INTEGER DEFAULT 25,
                             last_reminder_date DATE)''')
        
        self.cursor.execute('SELECT 1 FROM global_reminders LIMIT 1')
        if not self.cursor.fetchone():
            self.cursor.execute('INSERT INTO global_reminders (is_active, reminder_day) VALUES (1, 25)')
            self.conn.commit()

    def get_global_reminder_status(self) -> bool:
        self.cursor.execute('SELECT is_active FROM global_reminders LIMIT 1')
        result = self.cursor.fetchone()
        return result[0] if result else False

    def set_global_reminder_status(self, is_active: bool):
        self.cursor.execute('UPDATE global_reminders SET is_active = ?', (is_active,))
        self.conn.commit()

    def get_reminder_day(self) -> int:
        self.cursor.execute('SELECT reminder_day FROM global_reminders LIMIT 1')
        result = self.cursor.fetchone()
        return result[0] if result else 25

    def set_reminder_day(self, day: int):
        self.cursor.execute('UPDATE global_reminders SET reminder_day = ?', (day,))
        self.conn.commit()

    def get_users_for_reminder(self):
        if not self.get_global_reminder_status():
            return []
            
        today = datetime.now().date()
        reminder_day = self.get_reminder_day()
        
        if today.day != reminder_day:
            return []
            
        self.cursor.execute('''
            SELECT u.user_id 
            FROM users u
            WHERE NOT EXISTS (
                SELECT 1 FROM readings r 
                WHERE r.user_id = u.user_id 
                AND strftime('%Y-%m', r.date) = strftime('%Y-%m', ?)
            )
        ''', (today,))
        return [row[0] for row in self.cursor.fetchall()]

    def update_last_reminder_date(self):
        today = datetime.now().date()
        self.cursor.execute('UPDATE global_reminders SET last_reminder_date = ?', (today,))
        self.conn.commit()

    def get_user_readings(self, user_id: int, limit: int = None):
        query = '''
            SELECT value, strftime('%d.%m.%Y %H:%M', date) 
            FROM readings 
            WHERE user_id = ? 
            ORDER BY date DESC
        '''
        if limit:
            query += f' LIMIT {limit}'
        self.cursor.execute(query, (user_id,))
        return self.cursor.fetchall()

    def get_user_last_reading(self, user_id: int):
        self.cursor.execute('''
            SELECT value, strftime('%d.%m.%Y', date) 
            FROM readings 
            WHERE user_id = ? 
            ORDER BY date DESC 
            LIMIT 1
        ''', (user_id,))
        return self.cursor.fetchone()

    def get_user_by_id(self, user_id: int):
        self.cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return self.cursor.fetchone()

    def user_exists(self, user_id: int) -> bool:
        self.cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
        return bool(self.cursor.fetchone())

    def get_all_readings(self):
        self.cursor.execute('''
            SELECT u.user_id, u.full_name, u.plot_number, u.street, 
                   r.value, strftime('%d.%m.%Y %H:%M', r.date)
            FROM readings r
            JOIN users u ON r.user_id = u.user_id
            ORDER BY u.user_id, r.date DESC
        ''')
        return self.cursor.fetchall()

    def get_user_readings_for_export(self, user_id: int):
        self.cursor.execute('''
            SELECT u.full_name, u.plot_number, u.street, 
                   r.value, strftime('%d.%m.%Y %H:%M', r.date)
            FROM readings r
            JOIN users u ON r.user_id = u.user_id
            WHERE u.user_id = ?
            ORDER BY r.date DESC
        ''', (user_id,))
        return self.cursor.fetchall()

    def get_reading_by_id(self, reading_id: int):
        self.cursor.execute('''
            SELECT r.id, r.value, r.date, u.user_id, u.full_name 
            FROM readings r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.id = ?
        ''', (reading_id,))
        return self.cursor.fetchone()

    def update_reading_value(self, reading_id: int, new_value: float):
        self.cursor.execute('''
            UPDATE readings 
            SET value = ?, date = datetime('now')
            WHERE id = ?
        ''', (new_value, reading_id))
        self.conn.commit()

    def admin_update_reading_value(self, reading_id: int, new_value: float):
        self.cursor.execute('''
            UPDATE readings 
            SET value = ?, date = datetime('now')
            WHERE id = ?
        ''', (new_value, reading_id))
        self.conn.commit()

    def get_user_readings_with_ids(self, user_id: int):
        self.cursor.execute('''
            SELECT id, value, strftime('%d.%m.%Y %H:%M', date)
            FROM readings
            WHERE user_id = ?
            ORDER BY date DESC
        ''', (user_id,))
        return self.cursor.fetchall()

    def search_users(self, search_term: str):
        self.cursor.execute('''
            SELECT user_id, full_name, plot_number, street 
            FROM users 
            WHERE full_name LIKE ? OR plot_number LIKE ? OR street LIKE ?
        ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
        return self.cursor.fetchall()
    
    def update_user_field(self, user_id: int, field_name: str, new_value: str):
        if field_name not in ['full_name', 'plot_number', 'street']:
            return False
            
        try:
            self.cursor.execute(
                f"UPDATE users SET {field_name} = ? WHERE user_id = ?",
                (new_value, user_id)
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при обновлении пользователя: {e}")
            return False
    
    def get_all_users(self):
        self.cursor.execute('SELECT user_id, full_name, plot_number, street FROM users')
        return self.cursor.fetchall()
        
    def add_user(self, user_id: int, full_name: str, plot_number: str, street: str):
        try:
            self.cursor.execute(
                "INSERT INTO users (user_id, full_name, plot_number, street) VALUES (?, ?, ?, ?)",
                (user_id, full_name, plot_number, street)
            )
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при добавлении пользователя: {e}")
            return False
            
    def delete_user(self, user_id: int):
        try:
            self.cursor.execute("DELETE FROM readings WHERE user_id = ?", (user_id,))
            self.cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка при удалении пользователя: {e}")
            return False

async def sync_user_readings(bot: Bot, db: Database, user_id: int):
    """Синхронизирует локальные показания с сервером"""
    local_readings = LocalStorage.get_readings(user_id)
    
    if not local_readings:
        return True
        
    user = db.get_user_by_id(user_id)
    if not user:
        return False
        
    try:
        last_server_reading = db.get_user_last_reading(user_id)
        last_value = last_server_reading[0] if last_server_reading else 0
        
        local_readings.sort(key=lambda x: x["timestamp"])
        success_count = 0
        
        for reading in local_readings:
            value = float(reading["value"])
            
            if value > last_value:
                db.cursor.execute(
                    "INSERT INTO readings (user_id, value, date) VALUES (?, ?, ?)",
                    (user_id, value, reading["timestamp"])
                )
                last_value = value
                success_count += 1
                
        db.conn.commit()
        
        if success_count > 0:
            LocalStorage.clear_readings(user_id)
            await bot.send_message(
                user_id,
                f"🔁 Успешно синхронизировано {success_count} показаний с сервером"
            )
        return True
    except Exception as e:
        print(f"Ошибка синхронизации для пользователя {user_id}: {e}")
        return False

async def check_all_local_readings(bot: Bot, db: Database):
    """Проверяет и синхронизирует локальные данные всех пользователей"""
    for file in STORAGE_DIR.glob("*.json"):
        user_id = int(file.stem)
        try:
            await sync_user_readings(bot, db, user_id)
        except Exception as e:
            print(f"Ошибка синхронизации для пользователя {user_id}: {e}")

async def send_reminders(bot: Bot, db: Database):
    users_to_remind = db.get_users_for_reminder()
    
    for user_id in users_to_remind:
        try:
            await bot.send_message(
                user_id,
                "⏰ Ежемесячное напоминание!\n"
                "Пожалуйста, передайте текущие показания электросчетчика.\n"
                "Просто отправьте число - текущие показания в кВт·ч"
            )
        except Exception as e:
            print(f"Ошибка отправки напоминания пользователю {user_id}: {e}")
    
    if users_to_remind:
        db.update_last_reminder_date()

async def reminder_scheduler(bot: Bot, db: Database):
    while True:
        now = datetime.now()
        if now.hour == 12:
            await send_reminders(bot, db)
        await asyncio.sleep(3600)

async def notify_all_users(bot: Bot, db: Database):
    """Функция для оповещения всех пользователей при запуске бота"""
    users = db.get_all_users()
    if not users:
        print("Нет пользователей для оповещения")
        return
        
    success_count = 0
    fail_count = 0
    
    for user_id, full_name, plot_number, street in users:
        try:
            await bot.send_message(
                user_id,
                "🔔 Бот снова в сети и готов к работе!\n\n"
                "Теперь вы можете передавать показания счетчика, просто отправив число.\n\n"
                "Если у вас есть вопросы, обратитесь к администратору в чате СНТ Респиратор."
            )
            success_count += 1
            await asyncio.sleep(0.1)  # Небольшая задержка между сообщениями
        except Exception as e:
            print(f"Ошибка отправки уведомления пользователю {user_id} ({full_name}): {e}")
            fail_count += 1
    
    print(f"Оповещение пользователей завершено. Успешно: {success_count}, не удалось: {fail_count}")

def get_main_keyboard():
    """Главное меню с кнопками"""
    keyboard = [
        [types.KeyboardButton(text="📋 История показаний")],
        [types.KeyboardButton(text="💳 Инструкция по оплате")],
    ]
    return types.ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

async def main():
    db = Database()
    bot = Bot(token='7823293404:AAH61k_6YEcvSLuFkCfpjKtCpFcoNLydWOo')  # ⚠️ актуальный токен
    dp = Dispatcher(storage=MemoryStorage())
    admin_filter = IsAdminFilter()

    # При запуске проверяем локальные данные и оповещаем пользователей
    try:
        await notify_all_users(bot, db)
        await check_all_local_readings(bot, db)
    except Exception as e:
        print(f"Ошибка при отправке стартовых уведомлений: {e}")

    asyncio.create_task(reminder_scheduler(bot, db))

    # Вспомогательная функция для показа меню редактирования пользователя
    async def show_user_edit_menu(message: types.Message, user: tuple, state: FSMContext):
        user_id, full_name, plot_number, street = user
        await state.update_data(user_id=user_id)
        
        await message.answer(
            f"🔧 Редактирование пользователя:\n"
            f"ID: {user_id}\n"
            f"1. ФИО: {full_name}\n"
            f"2. Участок: {plot_number}\n"
            f"3. Улица: {street}\n\n"
            "Выберите номер поля для редактирования:",
            reply_markup=types.ReplyKeyboardMarkup(
                keyboard=[
                    [types.KeyboardButton(text="1")],
                    [types.KeyboardButton(text="2")],
                    [types.KeyboardButton(text="3")],
                    [types.KeyboardButton(text="Отменить")],
                ],
                resize_keyboard=True
            )
        )
        await state.set_state(AdminEditStates.select_user_field)

    @dp.message(Command("start"))
    async def start(message: types.Message):
        await message.answer(
            "Добро пожаловать! Отправьте показания счетчика или используйте кнопки ниже:",
            reply_markup=get_main_keyboard()
        )

    @dp.message(Command("register"))
    async def register_start(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        if db.user_exists(user_id):
            await message.answer("Вы уже зарегистрированы!", reply_markup=get_main_keyboard())
            return
        
        await message.answer(
            "📝 Начнем регистрацию. Пожалуйста, введите ваше ФИО (Фамилия Имя Отчество):",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(RegistrationStates.waiting_for_full_name)

    @dp.message(RegistrationStates.waiting_for_full_name)
    async def process_full_name(message: types.Message, state: FSMContext):
        full_name = message.text.strip()
        if len(full_name.split()) < 2:
            await message.answer("❌ Пожалуйста, введите Фамилию, Имя и Отчество (минимум Фамилию и Имя)")
            return
        
        await state.update_data(full_name=full_name)
        await message.answer(
            "🏠 Теперь введите название вашей улицы:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(RegistrationStates.waiting_for_street)

    @dp.message(RegistrationStates.waiting_for_street)
    async def process_street(message: types.Message, state: FSMContext):
        street = message.text.strip()
        if not street:
            await message.answer("❌ Название улицы не может быть пустым")
            return
        
        await state.update_data(street=street)
        await message.answer(
            "🔢 Теперь введите номер вашего участка:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(RegistrationStates.waiting_for_plot_number)

    @dp.message(RegistrationStates.waiting_for_plot_number)
    async def process_plot_number(message: types.Message, state: FSMContext):
        plot_number = message.text.strip()
        if not plot_number:
            await message.answer("❌ Номер участка не может быть пустым")
            return
        
        data = await state.get_data()
        full_name = data['full_name']
        street = data['street']
        
        success = db.add_user(
            user_id=message.from_user.id,
            full_name=full_name,
            plot_number=plot_number,
            street=street
        )
        
        if success:
            await message.answer(
                "✅ Регистрация успешно завершена!\n\n"
                f"📋 Ваши данные:\n"
                f"ФИО: {full_name}\n"
                f"Улица: {street}\n"
                f"Участок: {plot_number}\n\n"
                "Теперь вы можете отправлять показания счетчика, просто введя число.",
                reply_markup=get_main_keyboard()
            )
        else:
            await message.answer("❌ Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")
        
        await state.clear()

    @dp.message(Command("history"))
    async def show_history(message: types.Message):
        user = db.get_user_by_id(message.from_user.id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь командой /register", reply_markup=get_main_keyboard())
            return
        
        readings = db.get_user_readings(message.from_user.id, limit=10)
        
        if not readings:
            await message.answer("У вас пока нет сохраненных показаний", reply_markup=get_main_keyboard())
            return
        
        response = "📋 Ваша история показаний:\n\n"
        for idx, (value, date) in enumerate(readings, 1):
            response += f"{idx}. {value} кВт·ч - {date}\n"
        
        last_reading = db.get_user_last_reading(message.from_user.id)
        if last_reading:
            response += f"\nПоследнее показание: {last_reading[0]} кВт·ч ({last_reading[1]})"
        
        await message.answer(response, reply_markup=get_main_keyboard())

    @dp.message(Command("full_history"))
    async def show_full_history(message: types.Message):
        user = db.get_user_by_id(message.from_user.id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь командой /register", reply_markup=get_main_keyboard())
            return
        
        readings = db.get_user_readings(message.from_user.id)
        
        if not readings:
            await message.answer("У вас пока нет сохраненных показаний", reply_markup=get_main_keyboard())
            return
        
        if len(readings) > 15:
            df = pd.DataFrame(readings, columns=["Показание (кВт·ч)", "Дата"])
            filename = f"history_{message.from_user.id}.xlsx"
            df.to_excel(filename, index=False)
            
            await message.answer_document(
                FSInputFile(filename),
                caption="📊 Полная история ваших показаний",
                reply_markup=get_main_keyboard()
            )
            
            os.remove(filename)
        else:
            response = "📋 Полная история ваших показаний:\n\n"
            for idx, (value, date) in enumerate(readings, 1):
                response += f"{idx}. {value} кВт·ч - {date}\n"
            
            await message.answer(response, reply_markup=get_main_keyboard())

    @dp.message(Command("export"))
    async def export_data(message: types.Message):
        try:
            user = db.get_user_by_id(message.from_user.id)
            if not user:
                await message.answer("Сначала зарегистрируйтесь командой /register", reply_markup=get_main_keyboard())
                return
            
            data = db.get_user_readings_for_export(message.from_user.id)
            if not data:
                await message.answer("У вас нет сохраненных показаний для экспорта", reply_markup=get_main_keyboard())
                return
            
            # data приходит в порядке от новых к старым (DESC)
            # развернём для удобства вычисления разницы
            data_rev = list(reversed(data))  # теперь от старых к новым
            
            rows = []
            prev_value = None
            for full_name, plot_number, street, value, date in data_rev:
                if prev_value is None:
                    diff = "—"
                else:
                    diff = round(value - prev_value, 2)
                rows.append({
                    "ФИО": full_name,
                    "Улица": street,
                    "Участок": plot_number,
                    "Показание (кВт·ч)": value,
                    "Разница (кВт·ч)": diff,
                    "Дата": date
                })
                prev_value = value
            
            # Снова переворачиваем, чтобы в файле были от новых к старым
            rows.reverse()
            
            df = pd.DataFrame(rows)
            filename = f"показания_{message.from_user.id}.xlsx"
            df.to_excel(filename, index=False)
            
            await message.answer_document(
                FSInputFile(filename),
                caption="📊 Ваши показания (с разницей)",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка при экспорте: {str(e)}", reply_markup=get_main_keyboard())
        finally:
            if 'filename' in locals() and os.path.exists(filename):
                os.remove(filename)

    @dp.message(Command("admin_export"), admin_filter)
    async def admin_export(message: types.Message):
        try:
            data = db.get_all_readings()
            if not data:
                await message.answer("Нет данных для экспорта", reply_markup=get_main_keyboard())
                return
            
            # Группируем показания по пользователям
            user_readings = defaultdict(list)
            for user_id, full_name, plot_number, street, value, date in data:
                user_readings[user_id].append({
                    "full_name": full_name,
                    "plot_number": plot_number,
                    "street": street,
                    "value": value,
                    "date": date
                })
            
            rows = []
            for uid, readings in user_readings.items():
                # Сортируем показания по дате (от старых к новым)
                readings_sorted = sorted(readings, key=lambda x: x["date"])
                prev_value = None
                for r in readings_sorted:
                    diff = "—" if prev_value is None else round(r["value"] - prev_value, 2)
                    rows.append({
                        "ФИО": r["full_name"],
                        "Улица": r["street"],
                        "Участок": r["plot_number"],
                        "Показание (кВт·ч)": r["value"],
                        "Разница (кВт·ч)": diff,
                        "Дата": r["date"]
                    })
                    prev_value = r["value"]
            
            # Сортируем итоговые строки по убыванию даты (сначала новые)
            rows.sort(key=lambda x: x["Дата"], reverse=True)
            
            df = pd.DataFrame(rows)
            filename = "все_показания_с_разницей.xlsx"
            df.to_excel(filename, index=False)
            
            await message.answer_document(
                FSInputFile(filename),
                caption="📊 Все показания пользователей (с разницей)",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка выгрузки: {str(e)}", reply_markup=get_main_keyboard())
        finally:
            if 'filename' in locals() and os.path.exists(filename):
                os.remove(filename)

    @dp.message(Command("remind_on"), admin_filter)
    async def enable_reminders(message: types.Message):
        db.set_global_reminder_status(True)
        await message.answer(
            f"🔔 Глобальные напоминания включены!\n"
            f"Напоминания будут отправляться {db.get_reminder_day()} числа каждого месяца",
            reply_markup=get_main_keyboard()
        )

    @dp.message(Command("remind_off"), admin_filter)
    async def disable_reminders(message: types.Message):
        db.set_global_reminder_status(False)
        await message.answer("🔕 Глобальные напоминания отключены!", reply_markup=get_main_keyboard())

    @dp.message(Command("remind_status"), admin_filter)
    async def reminder_status(message: types.Message):
        status = "включены" if db.get_global_reminder_status() else "отключены"
        day = db.get_reminder_day()
        await message.answer(
            f"ℹ️ Статус напоминаний:\n"
            f"• Состояние: {status}\n"
            f"• День месяца: {day}",
            reply_markup=get_main_keyboard()
        )

    @dp.message(Command("set_remind_day"), admin_filter)
    async def set_reminder_day(message: types.Message):
        try:
            args = message.text.split()
            if len(args) < 2:
                raise ValueError("Не указан день")
            
            day = int(args[1])
            if not 1 <= day <= 28:
                await message.answer("❌ День должен быть от 1 до 28", reply_markup=get_main_keyboard())
                return
            
            db.set_reminder_day(day)
            await message.answer(
                f"📅 День напоминания установлен на {day} число каждого месяца\n"
                f"Следующее напоминание: {day}.{datetime.now().month + 1 if datetime.now().day >= day else datetime.now().month}.{datetime.now().year}",
                reply_markup=get_main_keyboard()
            )
        except (ValueError, IndexError):
            await message.answer("❌ Используйте: /set_remind_day <число от 1 до 28>", reply_markup=get_main_keyboard())

    @dp.message(Command("admin_edit"), admin_filter)
    async def admin_edit_start(message: types.Message, state: FSMContext):
        await message.answer(
            "Введите ID пользователя или его ФИО для поиска (редактирование показаний):",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.select_user_for_reading)

    @dp.message(AdminEditStates.select_user_for_reading, admin_filter)
    async def admin_edit_select_user(message: types.Message, state: FSMContext):
        search_term = message.text
        users = []
        
        if search_term.isdigit():
            user = db.get_user_by_id(int(search_term))
            if user:
                users = [user]
        
        if not users:
            users = db.search_users(search_term)
        
        if not users:
            await message.answer("Пользователь не найден", reply_markup=get_main_keyboard())
            await state.clear()
            return
        
        if len(users) == 1:
            user_id = users[0][0]
            readings = db.get_user_readings_with_ids(user_id)
            
            if not readings:
                await message.answer(f"У пользователя {users[0][1]} нет показаний", reply_markup=get_main_keyboard())
                await state.clear()
                return
                
            response = f"Показания пользователя {users[0][1]}:\n"
            for id, value, date in readings:
                response += f"ID:{id} - {value} кВт·ч ({date})\n"
            
            response += "\nВведите ID показания для редактирования:"
            await message.answer(response)
            await state.update_data(user_id=user_id)
            await state.set_state(AdminEditStates.select_reading)
        else:
            response = "Найдено несколько пользователей:\n"
            for idx, (user_id, full_name, plot_number, street) in enumerate(users, 1):
                response += f"{idx}. {full_name} (участок {plot_number}) [ID:{user_id}]\n"
            
            response += "\nВведите ID нужного пользователя:"
            await message.answer(response)

    @dp.message(AdminEditStates.select_reading, F.text.regexp(r'^\d+$'), admin_filter)
    async def admin_edit_select_reading(message: types.Message, state: FSMContext):
        data = await state.get_data()
        user_id = data.get('user_id')
        reading_id = int(message.text)
        
        reading = db.get_reading_by_id(reading_id)
        if not reading or (user_id and reading[3] != user_id):
            await message.answer("Показание с таким ID не найдено", reply_markup=get_main_keyboard())
            await state.clear()
            return
        
        await state.update_data(reading_id=reading_id)
        await message.answer(
            f"Текущее значение: {reading[1]} кВт·ч\n"
            f"Пользователь: {reading[4]}\n"
            f"Дата: {reading[2]}\n\n"
            "Введите новое значение (можно любое):"
        )
        await state.set_state(AdminEditStates.enter_new_reading_value)

    @dp.message(AdminEditStates.enter_new_reading_value, F.text.regexp(r'^\d+\.?\d*$'), admin_filter)
    async def admin_edit_finish(message: types.Message, state: FSMContext):
        data = await state.get_data()
        reading_id = data['reading_id']
        new_value = float(message.text)
        
        reading = db.get_reading_by_id(reading_id)
        old_value = reading[1]
        
        db.admin_update_reading_value(reading_id, new_value)
        await message.answer(
            f"✅ Показание изменено администратором!\n"
            f"ID: {reading_id}\n"
            f"Пользователь: {reading[4]}\n"
            f"Старое значение: {old_value} кВт·ч\n"
            f"Новое значение: {new_value} кВт·ч",
            reply_markup=get_main_keyboard()
        )
        await state.clear()

    @dp.message(Command("admin_force_edit"), admin_filter)
    async def admin_force_edit(message: types.Message, state: FSMContext):
        await message.answer(
            "⚙️ Режим принудительного редактирования\n"
            "Введите ID показания для изменения:"
        )
        await state.set_state(AdminEditStates.select_reading)

    @dp.message(Command("cancel_edit"), admin_filter)
    async def cancel_edit(message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("Редактирование отменено", reply_markup=get_main_keyboard())

    @dp.message(Command("admin_edit_user"), admin_filter)
    async def admin_edit_user_start(message: types.Message, state: FSMContext):
        await message.answer(
            "Введите ID пользователя или его ФИО/участок/улицу для поиска (редактирование данных пользователя):",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.select_user_for_edit)

    @dp.message(AdminEditStates.select_user_for_edit, admin_filter)
    async def admin_edit_user_select_user(message: types.Message, state: FSMContext):
        search_term = message.text.strip()
        users = []
        
        if search_term.isdigit():
            user = db.get_user_by_id(int(search_term))
            if user:
                users = [user]
        
        if not users:
            users = db.search_users(search_term)
        
        if not users:
            await message.answer("❌ Пользователь не найден", reply_markup=get_main_keyboard())
            await state.clear()
            return
        
        if len(users) == 1:
            user = users[0]
            await show_user_edit_menu(message, user, state)
        else:
            response = "Найдено несколько пользователей:\n"
            for idx, (user_id, full_name, plot_number, street) in enumerate(users, 1):
                response += f"{idx}. {full_name} (участок {plot_number}, ул.{street}) [ID:{user_id}]\n"
            
            response += "\nВведите ID нужного пользователя:"
            await message.answer(response)

    @dp.message(AdminEditStates.select_user_field, F.text.in_(["1", "2", "3"]), admin_filter)
    async def admin_edit_user_select_field(message: types.Message, state: FSMContext):
        field_map = {
            "1": ("full_name", "ФИО"),
            "2": ("plot_number", "номер участка"),
            "3": ("street", "улицу")
        }
        
        field_choice = message.text
        field_name, field_desc = field_map[field_choice]
        
        data = await state.get_data()
        user_id = data['user_id']
        user = db.get_user_by_id(user_id)
        
        if not user:
            await message.answer("❌ Пользователь не найден", reply_markup=get_main_keyboard())
            await state.clear()
            return
        
        current_value = user[1] if field_name == "full_name" else user[2] if field_name == "plot_number" else user[3]
        
        await state.update_data(
            field_name=field_name,
            field_desc=field_desc,
            current_value=current_value
        )
        
        await message.answer(
            f"Текущее значение {field_desc}: {current_value}\n"
            f"Введите новое значение:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.enter_new_user_value)

    @dp.message(AdminEditStates.select_user_field, F.text.casefold() == "отменить", admin_filter)
    async def cancel_user_edit(message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Редактирование отменено", reply_markup=types.ReplyKeyboardRemove())

    @dp.message(AdminEditStates.enter_new_user_value, admin_filter)
    async def admin_edit_user_finish(message: types.Message, state: FSMContext):
        data = await state.get_data()
        user_id = data['user_id']
        field_name = data['field_name']
        field_desc = data['field_desc']
        new_value = message.text.strip()
        
        if not new_value:
            await message.answer("❌ Значение не может быть пустым")
            return
        
        success = db.update_user_field(user_id, field_name, new_value)
        
        if success:
            await message.answer(
                f"✅ {field_desc.capitalize()} успешно изменено!\n"
                f"Новое значение: {new_value}"
            )
            
            user = db.get_user_by_id(user_id)
            if user:
                await show_user_edit_menu(message, user, state)
                return
        else:
            await message.answer("❌ Ошибка при обновлении данных")
        
        await state.clear()

    @dp.message(Command("admin_view_user"), admin_filter)
    async def admin_view_user(message: types.Message):
        try:
            args = message.text.split()
            if len(args) < 2:
                await message.answer("Используйте: /admin_view_user <ID пользователя>", reply_markup=get_main_keyboard())
                return
            
            user_id = int(args[1])
            user = db.get_user_by_id(user_id)
            
            if not user:
                await message.answer("Пользователь не найден", reply_markup=get_main_keyboard())
                return
            
            readings = db.get_user_readings(user_id, limit=3)
            readings_text = "\n".join([f"{value} кВт·ч ({date})" for value, date in readings])
            
            await message.answer(
                f"👤 Информация о пользователе:\n"
                f"ID: {user[0]}\n"
                f"ФИО: {user[1]}\n"
                f"Участок: {user[2]}\n"
                f"Улица: {user[3]}\n\n"
                f"Последние показания:\n{readings_text if readings else 'Нет показаний'}",
                reply_markup=get_main_keyboard()
            )
        except (ValueError, IndexError):
            await message.answer("Используйте: /admin_view_user <ID пользователя>", reply_markup=get_main_keyboard())

    @dp.message(Command("admin_list_users"), admin_filter)
    async def admin_list_users(message: types.Message):
        users = db.get_all_users()
        
        if not users:
            await message.answer("Нет зарегистрированных пользователей", reply_markup=get_main_keyboard())
            return
        
        response = "📋 Список пользователей:\n\n"
        for user_id, full_name, plot_number, street in users:
            response += f"ID:{user_id} - {full_name} (уч.{plot_number}, ул.{street})\n"
        
        if len(response) > 4000:
            for x in range(0, len(response), 4000):
                await message.answer(response[x:x+4000], reply_markup=get_main_keyboard())
        else:
            await message.answer(response, reply_markup=get_main_keyboard())

    # ==================== ИСПРАВЛЕННЫЙ БЛОК (4 шага) ====================
    @dp.message(Command("admin_add_user"), admin_filter)
    async def admin_add_user_start(message: types.Message, state: FSMContext):
        await message.answer(
            "Введите ID нового пользователя (число):",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.add_user_full_name)

    @dp.message(AdminEditStates.add_user_full_name, F.text.regexp(r'^\d+$'), admin_filter)
    async def admin_add_user_get_id(message: types.Message, state: FSMContext):
        user_id = int(message.text)
        if db.get_user_by_id(user_id):
            await message.answer("❌ Пользователь с таким ID уже существует")
            await state.clear()
            return
        await state.update_data(user_id=user_id)
        await message.answer("Введите ФИО нового пользователя:")
        await state.set_state(AdminEditStates.add_user_plot_number)

    @dp.message(AdminEditStates.add_user_plot_number, admin_filter)
    async def admin_add_user_get_full_name(message: types.Message, state: FSMContext):
        full_name = message.text.strip()
        if not full_name:
            await message.answer("❌ ФИО не может быть пустым")
            return
        await state.update_data(full_name=full_name)
        await message.answer("Введите номер участка нового пользователя:")
        await state.set_state(AdminEditStates.add_user_street)

    @dp.message(AdminEditStates.add_user_street, admin_filter)
    async def admin_add_user_get_plot_number(message: types.Message, state: FSMContext):
        plot_number = message.text.strip()
        if not plot_number:
            await message.answer("❌ Номер участка не может быть пустым")
            return
        await state.update_data(plot_number=plot_number)
        await message.answer("Введите улицу нового пользователя:")
        await state.set_state(AdminEditStates.add_user_street_name)  # Переходим в новое состояние для улицы

    @dp.message(AdminEditStates.add_user_street_name, admin_filter)
    async def admin_add_user_finish(message: types.Message, state: FSMContext):
        street = message.text.strip()
        if not street:
            await message.answer("❌ Улица не может быть пустой")
            return
        data = await state.get_data()
        user_id = data['user_id']
        full_name = data['full_name']
        plot_number = data['plot_number']
        
        success = db.add_user(user_id, full_name, plot_number, street)
        
        if success:
            await message.answer(
                f"✅ Пользователь успешно добавлен!\n"
                f"ID: {user_id}\n"
                f"ФИО: {full_name}\n"
                f"Участок: {plot_number}\n"
                f"Улица: {street}",
                reply_markup=get_main_keyboard()
            )
        else:
            await message.answer("❌ Ошибка при добавлении пользователя", reply_markup=get_main_keyboard())
        
        await state.clear()
    # ==================== КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ====================

    @dp.message(Command("admin_delete_user"), admin_filter)
    async def admin_delete_user_start(message: types.Message, state: FSMContext):
        await message.answer(
            "Введите ID пользователя для удаления или его ФИО/участок/улицу для поиска:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.select_user_for_delete)

    @dp.message(AdminEditStates.select_user_for_delete, admin_filter)
    async def admin_delete_user_select_user(message: types.Message, state: FSMContext):
        search_term = message.text.strip()
        users = []
        
        if search_term.isdigit():
            user = db.get_user_by_id(int(search_term))
            if user:
                users = [user]
        
        if not users:
            users = db.search_users(search_term)
        
        if not users:
            await message.answer("❌ Пользователь не найден", reply_markup=get_main_keyboard())
            await state.clear()
            return
        
        if len(users) == 1:
            user = users[0]
            await state.update_data(user_id=user[0])
            
            readings_count = len(db.get_user_readings(user[0]))
            
            await message.answer(
                f"Вы действительно хотите удалить пользователя?\n"
                f"ID: {user[0]}\n"
                f"ФИО: {user[1]}\n"
                f"Участок: {user[2]}\n"
                f"Улица: {user[3]}\n"
                f"Количество показаний: {readings_count}\n\n"
                "Это действие нельзя отменить!",
                reply_markup=types.ReplyKeyboardMarkup(
                    keyboard=[
                        [types.KeyboardButton(text="Да, удалить")],
                        [types.KeyboardButton(text="Отменить")]
                    ],
                    resize_keyboard=True
                )
            )
            await state.set_state(AdminEditStates.confirm_delete_user)
        else:
            response = "Найдено несколько пользователей:\n"
            for idx, (user_id, full_name, plot_number, street) in enumerate(users, 1):
                response += f"{idx}. {full_name} (участок {plot_number}, ул.{street}) [ID:{user_id}]\n"
            
            response += "\nВведите ID пользователя для удаления:"
            await message.answer(response)

    @dp.message(AdminEditStates.confirm_delete_user, F.text.casefold() == "да, удалить", admin_filter)
    async def admin_delete_user_confirm(message: types.Message, state: FSMContext):
        data = await state.get_data()
        user_id = data['user_id']
        user = db.get_user_by_id(user_id)
        
        if not user:
            await message.answer("❌ Пользователь не найден", reply_markup=types.ReplyKeyboardRemove())
            await state.clear()
            return
        
        success = db.delete_user(user_id)
        
        if success:
            await message.answer(
                f"✅ Пользователь успешно удален!\n"
                f"ID: {user[0]}\n"
                f"ФИО: {user[1]}\n"
                f"Участок: {user[2]}\n"
                f"Улица: {user[3]}",
                reply_markup=get_main_keyboard()
            )
        else:
            await message.answer("❌ Ошибка при удалении пользователя", reply_markup=get_main_keyboard())
        
        await state.clear()

    @dp.message(AdminEditStates.confirm_delete_user, F.text.casefold() == "отменить", admin_filter)
    async def admin_delete_user_cancel(message: types.Message, state: FSMContext):
        await state.clear()
        await message.answer("❌ Удаление отменено", reply_markup=get_main_keyboard())

    # ==================== ДОБАВЛЕНИЕ ПОКАЗАНИЙ АДМИНИСТРАТОРОМ ====================
    @dp.message(Command("admin_add_reading"), admin_filter)
    async def admin_add_reading_start(message: types.Message, state: FSMContext):
        await message.answer(
            "Введите ID пользователя, для которого хотите добавить показание:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await state.set_state(AdminEditStates.add_reading_user_id)

    @dp.message(AdminEditStates.add_reading_user_id, F.text.regexp(r'^\d+$'), admin_filter)
    async def admin_add_reading_get_user(message: types.Message, state: FSMContext):
        user_id = int(message.text)
        user = db.get_user_by_id(user_id)
        if not user:
            await message.answer("❌ Пользователь с таким ID не найден. Попробуйте ещё раз или /cancel_edit для отмены.")
            return
        await state.update_data(target_user_id=user_id, target_user_name=user[1])
        await message.answer(
            f"Пользователь: {user[1]} (уч.{user[2]}, ул.{user[3]})\n"
            "Введите новое показание (число в кВт·ч):"
        )
        await state.set_state(AdminEditStates.add_reading_value)

    @dp.message(AdminEditStates.add_reading_value, F.text.regexp(r'^\d+\.?\d*$'), admin_filter)
    async def admin_add_reading_finish(message: types.Message, state: FSMContext):
        data = await state.get_data()
        user_id = data['target_user_id']
        value = float(message.text)
        try:
            db.cursor.execute(
                "INSERT INTO readings (user_id, value) VALUES (?, ?)",
                (user_id, value)
            )
            db.conn.commit()
            await message.answer(
                f"✅ Показание {value} кВт·ч успешно добавлено для пользователя {data['target_user_name']} (ID {user_id}).",
                reply_markup=get_main_keyboard()
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка при добавлении показания: {e}", reply_markup=get_main_keyboard())
        finally:
            await state.clear()

    @dp.message(AdminEditStates.add_reading_user_id, admin_filter)
    async def admin_add_reading_invalid_id(message: types.Message, state: FSMContext):
        await message.answer("❌ ID должен быть числом. Попробуйте ещё раз или /cancel_edit для отмены.")

    @dp.message(AdminEditStates.add_reading_value, admin_filter)
    async def admin_add_reading_invalid_value(message: types.Message, state: FSMContext):
        await message.answer("❌ Показание должно быть числом (можно с десятичной точкой). Попробуйте ещё раз или /cancel_edit для отмены.")
    # ==================== КОНЕЦ БЛОКА ====================

    # Обработчик кнопки "История показаний"
    @dp.message(F.text == "📋 История показаний")
    async def history_button_handler(message: types.Message):
        user = db.get_user_by_id(message.from_user.id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь командой /register", reply_markup=get_main_keyboard())
            return
        readings = db.get_user_readings(message.from_user.id, limit=10)
        if not readings:
            await message.answer("У вас пока нет сохраненных показаний", reply_markup=get_main_keyboard())
            return
        response = "📋 Ваша история показаний:\n\n"
        for idx, (value, date) in enumerate(readings, 1):
            response += f"{idx}. {value} кВт·ч - {date}\n"
        last_reading = db.get_user_last_reading(message.from_user.id)
        if last_reading:
            response += f"\nПоследнее показание: {last_reading[0]} кВт·ч ({last_reading[1]})"
        await message.answer(response, reply_markup=get_main_keyboard())

    # Обработчик кнопки "Инструкция по оплате"
    @dp.message(F.text == "💳 Инструкция по оплате")
    async def payment_button_handler(message: types.Message):
        text = (
            "💳 **Уважаемые садоводы!**\n\n"
            "Вы можете производить оплату членских взносов и за электроэнергию через **ПАО «ПРОМСВЯЗЬБАНК»** "
            "на расчетный счет СНТ «Респиратор».\n\n"
            "📌 **При заполнении платежа обязательно указывайте:**\n"
            "• ФИО плательщика\n"
            "• Номер участка\n"
            "• Назначение взноса (членские / целевые / электроэнергия)\n"
            "• Сумму и период оплаты\n"
            "• При оплате за электроэнергию – начальные и конечные показания счётчика\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "**РЕКВИЗИТЫ СЧЁТА:**\n"
            "```\n"
            "Садоводческое некоммерческое товарищество «Респиратор»\n"
            "ИНН 9308021183\n"
            "ОГРН 1229300156799\n"
            "Р/с 40703810609300009078\n"
            "БИК 044525555\n"
            "```\n"
            "После оплаты, пожалуйста, отправьте **скриншот или фото квитанции** в этот чат. "
            "Администратор проверит поступление средств и отметит платеж."
        )
        await message.answer(text, parse_mode="Markdown", reply_markup=get_main_keyboard())

    @dp.message(lambda m: m.text.replace('.', '').isdigit())
    async def save_reading(message: types.Message):
        user = db.get_user_by_id(message.from_user.id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь командой /register", reply_markup=get_main_keyboard())
            return
        
        try:
            last_reading = db.get_user_last_reading(message.from_user.id)
            current_reading = float(message.text)
            
            if last_reading and current_reading <= last_reading[0]:
                await message.answer(f"❌ Ошибка: показание должно быть больше последнего ({last_reading[0]} кВт·ч)", reply_markup=get_main_keyboard())
                return
            
            # Пытаемся сохранить в основную БД
            try:
                db.cursor.execute(
                    "INSERT INTO readings (user_id, value) VALUES (?, ?)",
                    (message.from_user.id, current_reading)
                )
                db.conn.commit()
                
                # Если успешно, проверяем есть ли локальные данные для синхронизации
                if LocalStorage.get_readings(message.from_user.id):
                    await sync_user_readings(bot, db, message.from_user.id)
                
                await message.answer(f"✅ Показание {current_reading} кВт·ч сохранено!", reply_markup=get_main_keyboard())
            except sqlite3.Error as e:
                # Если ошибка БД, сохраняем локально
                print(f"Ошибка БД: {e}. Сохраняем локально.")
                LocalStorage.save_reading(message.from_user.id, current_reading)
                await message.answer(
                    f"⚠️ Сервер временно недоступен. Показание {current_reading} кВт·ч сохранено локально.\n"
                    "Оно будет автоматически отправлено при восстановлении связи.",
                    reply_markup=get_main_keyboard()
                )
        except Exception as e:
            await message.answer("❌ Произошла ошибка при обработке показания", reply_markup=get_main_keyboard())
            print(f"Ошибка в save_reading: {e}")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        db.conn.close()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")