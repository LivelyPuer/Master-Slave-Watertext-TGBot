import asyncio
import os
import json
import logging
from io import BytesIO
from typing import Dict, Optional, List

from pathlib import Path
import zipfile
import shutil
import tempfile

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, BufferedInputFile
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# ============= КОНФИГУРАЦИЯ =============
MASTER_TOKEN = os.getenv("MASTER_BOT_TOKEN")
MASTER_PASSWORD = os.getenv("MASTER_PASSWORD")
SLAVES_DB_FILE = "slaves_database.json"

# Хранилище активных slave ботов
active_slaves: Dict[str, Bot] = {}
slave_watermarks: Dict[str, str] = {}
slave_watermark_settings: Dict[str, Dict] = {}  # Настройки водяного знака для каждого slave
slave_dispatchers: Dict[str, Dispatcher] = {}
slave_tasks: List[asyncio.Task] = []  # Задачи polling для slave ботов

# Структура настроек водяного знака по умолчанию
DEFAULT_WATERMARK_SETTINGS = {
    "size_percent": 0.3,  # 30% от минимальной стороны
    "color_r": 255,  # Белый цвет
    "color_g": 255,
    "color_b": 255,
    "opacity": 128,  # 50% прозрачности (0-255)
    "auto_color": False,  # Автоматическое определение цвета
    "stroke_enabled": False,  # Включена ли обводка
    "stroke_width": 2  # Толщина обводки в пикселях
}


# ============= БАЗА ДАННЫХ SLAVE БОТОВ =============
def load_slaves_from_db() -> List[Dict[str, str]]:
    """Загружает список slave ботов из JSON файла"""
    if not Path(SLAVES_DB_FILE).exists():
        logger.info(f"Файл базы данных {SLAVES_DB_FILE} не найден, создаем новый")
        return []
    
    try:
        with open(SLAVES_DB_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"Загружено {len(data)} slave ботов из базы данных")
            return data
    except Exception as e:
        logger.error(f"Ошибка загрузки базы данных: {e}", exc_info=True)
        return []


def save_slaves_to_db():
    """Сохраняет список slave ботов в JSON файл"""
    slaves_data = []
    
    for token, bot in active_slaves.items():
        slave_data = {
            "token": token,
            "watermark": slave_watermarks.get(token, ""),
            "settings": slave_watermark_settings.get(token, DEFAULT_WATERMARK_SETTINGS.copy())
        }
        slaves_data.append(slave_data)
    
    try:
        with open(SLAVES_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(slaves_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Сохранено {len(slaves_data)} slave ботов в базу данных")
    except Exception as e:
        logger.error(f"Ошибка сохранения базы данных: {e}", exc_info=True)


async def restore_slaves_from_db():
    """Восстанавливает slave ботов из базы данных при запуске"""
    slaves_data = load_slaves_from_db()
    
    if not slaves_data:
        logger.info("Нет slave ботов для восстановления")
        return
    
    logger.info(f"Восстановление {len(slaves_data)} slave ботов...")
    
    for slave_info in slaves_data:
        token = slave_info.get("token")
        watermark = slave_info.get("watermark", "")
        settings = slave_info.get("settings", DEFAULT_WATERMARK_SETTINGS.copy())
        
        if not token:
            logger.warning("Пропущена запись без токена")
            continue
        
        try:
            logger.info(f"Восстановление slave бота с водяным знаком: {watermark}")
            # Сохраняем настройки перед запуском
            slave_watermark_settings[token] = settings
            task = await start_slave_bot(token, watermark, save_to_db=False)
            slave_tasks.append(task)
            logger.info(f"✅ Slave бот восстановлен успешно")
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления slave бота: {e}", exc_info=True)


# ============= FSM СОСТОЯНИЯ =============
class MasterStates(StatesGroup):
    waiting_password = State()
    waiting_slave_token = State()
    waiting_watermark = State()
    waiting_slave_selection = State()  # Выбор slave бота для настройки
    waiting_watermark_text = State()  # Изменение текста водяного знака
    waiting_size_percent = State()  # Настройка размера
    waiting_color = State()  # Настройка цвета (RGB)
    waiting_opacity = State()  # Настройка прозрачности
    waiting_stroke_width = State()  # Настройка толщины обводки
    waiting_test_image = State()  # Ожидание тестового изображения


class SlaveStates(StatesGroup):
    processing_images = State()


# ============= MASTER BOT =============
master_router = Router()
authenticated_users = set()
selected_slave_tokens: Dict[int, str] = {}  # Хранилище выбранных slave ботов для каждого пользователя


@master_router.message(CommandStart())
async def master_start(message: Message, state: FSMContext):
    logger.info(f"Master bot: команда /start от пользователя {message.from_user.id}")
    if message.from_user.id in authenticated_users:
        await message.answer(
            "🤖 Вы уже авторизованы!\n\n"
            "Доступные команды:\n"
            "/create_slave - Создать нового slave бота\n"
            "/list_slaves - Список активных slave ботов\n"
            "/configure_slave - Настроить водяной знак slave бота\n"
            "/test_watermark - Отправить тестовое изображение\n"
            "/stop_slave - Остановить slave бота"
        )
    else:
        await message.answer("🔐 Введите пароль для доступа к master боту:")
        await state.set_state(MasterStates.waiting_password)


@master_router.message(MasterStates.waiting_password)
async def check_password(message: Message, state: FSMContext):
    logger.info(f"Проверка пароля от пользователя {message.from_user.id}")
    if message.text == MASTER_PASSWORD:
        authenticated_users.add(message.from_user.id)
        await state.clear()
        logger.info(f"Пользователь {message.from_user.id} успешно авторизован")
        await message.answer(
            "✅ Авторизация успешна!\n\n"
            "Доступные команды:\n"
            "/create_slave - Создать нового slave бота\n"
            "/list_slaves - Список активных slave ботов\n"
            "/configure_slave - Настроить водяной знак slave бота\n"
            "/test_watermark - Отправить тестовое изображение\n"
            "/stop_slave - Остановить slave бота"
        )
    else:
        logger.warning(f"Неверный пароль от пользователя {message.from_user.id}")
        await message.answer("❌ Неверный пароль. Попробуйте еще раз:")


@master_router.message(Command("create_slave"))
async def create_slave_start(message: Message, state: FSMContext):
    logger.info(f"Команда /create_slave от пользователя {message.from_user.id}")
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен. Используйте /start для авторизации.")
        return
    
    await message.answer("🔑 Отправьте токен для нового slave бота:")
    await state.set_state(MasterStates.waiting_slave_token)


@master_router.message(MasterStates.waiting_slave_token)
async def receive_slave_token(message: Message, state: FSMContext):
    token = message.text.strip()
    logger.info(f"Получен токен slave бота (длина: {len(token)})")
    
    try:
        test_bot = Bot(token=token)
        bot_info = await test_bot.get_me()
        await test_bot.session.close()
        
        logger.info(f"Токен валиден. Бот: @{bot_info.username}")
        await state.update_data(slave_token=token, bot_username=bot_info.username)
        await message.answer(
            f"✅ Токен валиден! Бот: @{bot_info.username}\n\n"
            "📝 Теперь введите текст водяного знака:"
        )
        await state.set_state(MasterStates.waiting_watermark)
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка проверки токена: {str(e)}\n\nПопробуйте еще раз:")


@master_router.message(MasterStates.waiting_watermark)
async def receive_watermark(message: Message, state: FSMContext):
    watermark_text = message.text
    data = await state.get_data()
    token = data['slave_token']
    bot_username = data['bot_username']
    
    logger.info(f"Создание slave бота @{bot_username} с водяным знаком: {watermark_text}")
    
    # Инициализируем настройки по умолчанию
    slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
    
    task = await start_slave_bot(token, watermark_text, save_to_db=True)
    slave_tasks.append(task)
    
    await message.answer(
        f"🎉 Slave бот успешно создан и запущен!\n\n"
        f"🤖 Бот: @{bot_username}\n"
        f"💧 Водяной знак: {watermark_text}\n\n"
        f"Теперь вы можете отправлять изображения этому боту.\n"
        f"💾 Данные сохранены в базу данных.\n\n"
        f"Используйте /configure_slave для настройки водяного знака."
    )
    await state.clear()


@master_router.message(Command("list_slaves"))
async def list_slaves(message: Message):
    logger.info(f"Команда /list_slaves от пользователя {message.from_user.id}")
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен. Используйте /start для авторизации.")
        return
    
    if not active_slaves:
        await message.answer("📋 Нет активных slave ботов.")
        return
    
    response = "📋 Активные slave боты:\n\n"
    for i, (token, bot) in enumerate(active_slaves.items(), 1):
        bot_info = await bot.get_me()
        watermark = slave_watermarks.get(token, "N/A")
        response += f"{i}. @{bot_info.username}\n   💧 Водяной знак: {watermark}\n\n"
    
    logger.info(f"Активных slave ботов: {len(active_slaves)}")
    await message.answer(response)


@master_router.message(Command("stop_slave"))
async def stop_slave(message: Message):
    logger.info(f"Команда /stop_slave от пользователя {message.from_user.id}")
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен. Используйте /start для авторизации.")
        return
    
    await message.answer(
        "⚠️ Функция остановки slave ботов доступна.\n"
        "Для остановки перезапустите master бота."
    )


@master_router.message(Command("configure_slave"))
async def configure_slave_start(message: Message, state: FSMContext):
    logger.info(f"Команда /configure_slave от пользователя {message.from_user.id}")
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен. Используйте /start для авторизации.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов. Создайте бота командой /create_slave")
        return
    
    # Формируем список slave ботов
    response = "📋 Выберите slave бота для настройки:\n\n"
    bot_list = []
    for i, (token, bot) in enumerate(active_slaves.items(), 1):
        bot_info = await bot.get_me()
        watermark = slave_watermarks.get(token, "N/A")
        bot_list.append((token, bot_info.username))
        response += f"{i}. @{bot_info.username}\n   💧 Водяной знак: {watermark}\n\n"
    
    await state.update_data(bot_list=bot_list)
    await message.answer(response + "Введите номер бота (1, 2, 3...):")
    await state.set_state(MasterStates.waiting_slave_selection)


@master_router.message(MasterStates.waiting_slave_selection)
async def receive_slave_selection(message: Message, state: FSMContext):
    try:
        bot_number = int(message.text.strip())
        data = await state.get_data()
        bot_list = data.get('bot_list', [])
        
        if bot_number < 1 or bot_number > len(bot_list):
            await message.answer(f"❌ Неверный номер. Введите число от 1 до {len(bot_list)}:")
            return
        
        token, username = bot_list[bot_number - 1]
        settings = slave_watermark_settings.get(token, DEFAULT_WATERMARK_SETTINGS.copy())
        watermark = slave_watermarks.get(token, "")
        
        # Сохраняем выбранный бот для пользователя
        selected_slave_tokens[message.from_user.id] = token
        
        auto_color_status = "✅ Включено" if settings.get('auto_color', False) else "❌ Выключено"
        color_info = "Автоматический (белый/черный)" if settings.get('auto_color', False) else f"RGB({settings['color_r']}, {settings['color_g']}, {settings['color_b']})"
        stroke_enabled = settings.get('stroke_enabled', False)
        stroke_width = settings.get('stroke_width', 2)
        stroke_status = f"✅ Включена (толщина: {stroke_width}px)" if stroke_enabled else "❌ Выключена"
        
        response = (
            f"⚙️ Настройка водяного знака для @{username}\n\n"
            f"📝 Текущий текст: {watermark}\n"
            f"📏 Размер: {settings['size_percent']*100:.0f}% от минимальной стороны\n"
            f"🎨 Цвет: {color_info}\n"
            f"🤖 Автоматический цвет: {auto_color_status}\n"
            f"👻 Прозрачность: {int(settings['opacity']/255*100)}%\n"
            f"🖊️ Обводка: {stroke_status}\n\n"
            f"Выберите параметр для изменения:\n"
            f"1️⃣ /set_text - Изменить текст\n"
            f"2️⃣ /set_size - Изменить размер (0.1-1.0)\n"
            f"3️⃣ /set_color - Изменить цвет (R G B, например: 255 255 255)\n"
            f"4️⃣ /set_opacity - Изменить прозрачность (0-100%)\n"
            f"5️⃣ /set_auto_color - Включить/выключить автоматический цвет\n"
            f"6️⃣ /set_stroke - Включить/выключить обводку\n"
            f"7️⃣ /set_stroke_width - Изменить толщину обводки\n"
            f"8️⃣ /test_watermark - Отправить тестовое изображение"
        )
        await message.answer(response)
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число (1, 2, 3...):")


@master_router.message(Command("set_text"))
async def set_text_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("📝 Введите новый текст водяного знака:")
    await state.set_state(MasterStates.waiting_watermark_text)


@master_router.message(MasterStates.waiting_watermark_text)
async def receive_watermark_text(message: Message, state: FSMContext):
    new_text = message.text
    token = selected_slave_tokens.get(message.from_user.id)
    
    if not token or token not in active_slaves:
        await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
        await state.clear()
        return
    
    slave_watermarks[token] = new_text
    save_slaves_to_db()
    
    # Перезапускаем slave бота с новым текстом
    bot = active_slaves[token]
    bot_info = await bot.get_me()
    
    # Останавливаем старый dispatcher и создаем новый
    old_dp = slave_dispatchers.get(token)
    if old_dp:
        await old_dp.stop_polling()
    
    # Создаем новый router с обновленным текстом
    storage = MemoryStorage()
    new_dp = Dispatcher(storage=storage)
    router = create_slave_router(new_text, token)
    new_dp.include_router(router)
    slave_dispatchers[token] = new_dp
    
    # Перезапускаем polling
    task = asyncio.create_task(new_dp.start_polling(bot, handle_signals=False))
    # Находим и заменяем старую задачу
    for i, t in enumerate(slave_tasks):
        if not t.done():
            t.cancel()
            slave_tasks[i] = task
            break
    else:
        slave_tasks.append(task)
    
    await message.answer(f"✅ Текст водяного знака обновлен: {new_text}")
    await state.clear()
    
    # Отправляем тестовое изображение
    await send_test_preview(message, token)


@master_router.message(Command("set_size"))
async def set_size_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("📏 Введите размер водяного знака (0.1-1.0, например 0.3 для 30%):")
    await state.set_state(MasterStates.waiting_size_percent)


@master_router.message(MasterStates.waiting_size_percent)
async def receive_size_percent(message: Message, state: FSMContext):
    try:
        size = float(message.text.strip())
        if size < 0.1 or size > 1.0:
            await message.answer("❌ Размер должен быть от 0.1 до 1.0. Попробуйте еще раз:")
            return
        
        token = selected_slave_tokens.get(message.from_user.id)
        
        if not token or token not in active_slaves:
            await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
            await state.clear()
            return
        
        if token not in slave_watermark_settings:
            slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
        
        slave_watermark_settings[token]['size_percent'] = size
        save_slaves_to_db()
        
        await message.answer(f"✅ Размер водяного знака обновлен: {size*100:.0f}%")
        await state.clear()
        
        # Отправляем тестовое изображение
        await send_test_preview(message, token)
    except ValueError:
        await message.answer("❌ Введите число (например: 0.3):")


@master_router.message(Command("set_color"))
async def set_color_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("🎨 Введите цвет в формате RGB (три числа от 0 до 255 через пробел):\nНапример: 255 255 255 (белый)")
    await state.set_state(MasterStates.waiting_color)


@master_router.message(MasterStates.waiting_color)
async def receive_color(message: Message, state: FSMContext):
    try:
        parts = message.text.strip().split()
        if len(parts) != 3:
            await message.answer("❌ Введите три числа через пробел (например: 255 255 255):")
            return
        
        r, g, b = int(parts[0]), int(parts[1]), int(parts[2])
        
        if not all(0 <= val <= 255 for val in [r, g, b]):
            await message.answer("❌ Значения должны быть от 0 до 255. Попробуйте еще раз:")
            return
        
        token = selected_slave_tokens.get(message.from_user.id)
        
        if not token or token not in active_slaves:
            await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
            await state.clear()
            return
        
        if token not in slave_watermark_settings:
            slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
        
        slave_watermark_settings[token]['color_r'] = r
        slave_watermark_settings[token]['color_g'] = g
        slave_watermark_settings[token]['color_b'] = b
        save_slaves_to_db()
        
        await message.answer(f"✅ Цвет обновлен: RGB({r}, {g}, {b})")
        await state.clear()
        
        # Отправляем тестовое изображение
        await send_test_preview(message, token)
    except ValueError:
        await message.answer("❌ Введите три числа от 0 до 255 через пробел:")


@master_router.message(Command("set_opacity"))
async def set_opacity_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("👻 Введите прозрачность (0-100%, где 0 - полностью прозрачный, 100 - непрозрачный):")
    await state.set_state(MasterStates.waiting_opacity)


@master_router.message(MasterStates.waiting_opacity)
async def receive_opacity(message: Message, state: FSMContext):
    try:
        opacity_percent = int(message.text.strip())
        if opacity_percent < 0 or opacity_percent > 100:
            await message.answer("❌ Прозрачность должна быть от 0 до 100. Попробуйте еще раз:")
            return
        
        opacity = int(opacity_percent / 100 * 255)
        
        token = selected_slave_tokens.get(message.from_user.id)
        
        if not token or token not in active_slaves:
            await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
            await state.clear()
            return
        
        if token not in slave_watermark_settings:
            slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
        
        slave_watermark_settings[token]['opacity'] = opacity
        save_slaves_to_db()
        
        await message.answer(f"✅ Прозрачность обновлена: {opacity_percent}%")
        await state.clear()
        
        # Отправляем тестовое изображение
        await send_test_preview(message, token)
    except ValueError:
        await message.answer("❌ Введите число от 0 до 100:")


@master_router.message(Command("set_auto_color"))
async def set_auto_color_toggle(message: Message):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    if token not in slave_watermark_settings:
        slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
    
    # Переключаем автоматический цвет
    current_auto_color = slave_watermark_settings[token].get('auto_color', False)
    new_auto_color = not current_auto_color
    slave_watermark_settings[token]['auto_color'] = new_auto_color
    save_slaves_to_db()
    
    status = "включен" if new_auto_color else "выключен"
    description = "Белый для темных изображений, черный для светлых" if new_auto_color else "Используется цвет из настроек"
    
    await message.answer(
        f"✅ Автоматический цвет {status}.\n\n"
        f"{description}\n\n"
        f"Используйте /configure_slave для просмотра всех настроек."
    )
    
    # Отправляем тестовое изображение
    await send_test_preview(message, token)


@master_router.message(Command("set_stroke"))
async def set_stroke_toggle(message: Message):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    if token not in slave_watermark_settings:
        slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
    
    # Переключаем обводку
    current_stroke = slave_watermark_settings[token].get('stroke_enabled', False)
    new_stroke = not current_stroke
    slave_watermark_settings[token]['stroke_enabled'] = new_stroke
    save_slaves_to_db()
    
    status = "включена" if new_stroke else "выключена"
    description = "Цвет обводки будет инвертированным цветом текста" if new_stroke else "Обводка отключена"
    
    await message.answer(
        f"✅ Обводка {status}.\n\n"
        f"{description}\n\n"
        f"Используйте /configure_slave для просмотра всех настроек."
    )
    
    # Отправляем тестовое изображение
    await send_test_preview(message, token)


@master_router.message(Command("set_stroke_width"))
async def set_stroke_width_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("🖊️ Введите толщину обводки в пикселях (1-10, рекомендуется 2-4):")
    await state.set_state(MasterStates.waiting_stroke_width)


@master_router.message(MasterStates.waiting_stroke_width)
async def receive_stroke_width(message: Message, state: FSMContext):
    try:
        stroke_width = int(message.text.strip())
        if stroke_width < 1 or stroke_width > 10:
            await message.answer("❌ Толщина обводки должна быть от 1 до 10. Попробуйте еще раз:")
            return
        
        token = selected_slave_tokens.get(message.from_user.id)
        
        if not token or token not in active_slaves:
            await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
            await state.clear()
            return
        
        if token not in slave_watermark_settings:
            slave_watermark_settings[token] = DEFAULT_WATERMARK_SETTINGS.copy()
        
        slave_watermark_settings[token]['stroke_width'] = stroke_width
        save_slaves_to_db()
        
        await message.answer(f"✅ Толщина обводки обновлена: {stroke_width}px")
        await state.clear()
        
        # Отправляем тестовое изображение
        await send_test_preview(message, token)
    except ValueError:
        await message.answer("❌ Введите число от 1 до 10:")


@master_router.message(Command("test_watermark"))
async def test_watermark_start(message: Message, state: FSMContext):
    if message.from_user.id not in authenticated_users:
        await message.answer("❌ Доступ запрещен.")
        return
    
    if not active_slaves:
        await message.answer("❌ Нет активных slave ботов.")
        return
    
    # Получаем выбранный бот
    token = selected_slave_tokens.get(message.from_user.id)
    if not token and len(active_slaves) == 1:
        token = list(active_slaves.keys())[0]
        selected_slave_tokens[message.from_user.id] = token
    
    if not token:
        await message.answer("❌ Сначала выберите бота командой /configure_slave")
        return
    
    await message.answer("📤 Отправьте тестовое изображение (как файл):")
    await state.set_state(MasterStates.waiting_test_image)


@master_router.message(MasterStates.waiting_test_image, F.document)
async def process_test_image(message: Message, state: FSMContext):
    doc = message.document
    
    if not doc.mime_type or not doc.mime_type.startswith('image/'):
        await message.answer("❌ Пожалуйста, отправьте файл изображения (JPEG, PNG и т.д.)")
        return
    
    token = selected_slave_tokens.get(message.from_user.id)
    
    if not token or token not in active_slaves:
        await message.answer("❌ Ошибка: бот не найден. Начните заново с /configure_slave")
        await state.clear()
        return
    
    watermark_text = slave_watermarks.get(token, "")
    settings = slave_watermark_settings.get(token, DEFAULT_WATERMARK_SETTINGS.copy())
    
    try:
        # Получаем файл
        file = await message.bot.get_file(doc.file_id)
        file_bytes = await message.bot.download_file(file.file_path)
        
        # Обрабатываем изображение с настройками
        processed_image = await process_image_with_watermark(
            file_bytes.read(),
            watermark_text,
            settings
        )
        
        # Отправляем обработанное изображение
        input_file = BufferedInputFile(
            processed_image,
            filename=f"test_watermarked_{doc.file_name}"
        )
        
        await message.answer_document(document=input_file)
        await message.answer("✅ Тестовое изображение обработано с текущими настройками водяного знака.")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка обработки тестового изображения: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка обработки: {str(e)}")
        await state.clear()


# ============= SLAVE BOT ЛОГИКА =============
def calculate_average_brightness(img: Image.Image) -> float:
    """Вычисляет среднюю яркость изображения (0-255)"""
    # Конвертируем в RGB, если нужно
    if img.mode != 'RGB':
        img = img.convert('RGB')
    
    # Получаем пиксели
    pixels = list(img.getdata())
    
    # Вычисляем среднюю яркость используя формулу восприятия яркости
    # L = 0.299*R + 0.587*G + 0.114*B
    total_brightness = 0
    for r, g, b in pixels:
        brightness = 0.299 * r + 0.587 * g + 0.114 * b
        total_brightness += brightness
    
    average_brightness = total_brightness / len(pixels)
    logger.info(f"Средняя яркость изображения: {average_brightness:.2f}")
    return average_brightness


def get_auto_color(img: Image.Image) -> tuple:
    """Определяет цвет водяного знака на основе яркости изображения"""
    brightness = calculate_average_brightness(img)
    
    # Порог яркости: если средняя яркость меньше 128, изображение темное - используем белый
    # Если больше или равно 128, изображение светлое - используем черный
    if brightness < 128:
        color = (255, 255, 255)  # Белый для темных изображений
        logger.info("Изображение темное, выбран белый цвет")
    else:
        color = (0, 0, 0)  # Черный для светлых изображений
        logger.info("Изображение светлое, выбран черный цвет")
    
    return color


def invert_color(color: tuple) -> tuple:
    """Инвертирует цвет (белый -> черный, черный -> белый)"""
    r, g, b = color
    inverted = (255 - r, 255 - g, 255 - b)
    logger.info(f"Инвертирован цвет: RGB{color} -> RGB{inverted}")
    return inverted


async def generate_test_image(watermark_text: str, settings: Dict) -> bytes:
    """Генерирует тестовое изображение с белым и черным фоном пополам"""
    # Создаем изображение 800x400 (белый и черный фон пополам)
    width, height = 800, 400
    img = Image.new('RGB', (width, height), (255, 255, 255))
    
    # Рисуем черную половину (правая часть)
    draw = ImageDraw.Draw(img)
    draw.rectangle([width // 2, 0, width, height], fill=(0, 0, 0))
    
    # Конвертируем в RGBA для добавления водяного знака
    img = img.convert('RGBA')
    
    # Создаем копию для обработки
    img_copy = img.copy()
    
    # Обрабатываем изображение с водяным знаком (увеличиваем и добавляем водяной знак)
    img_bytes = BytesIO()
    img_copy.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    
    # Используем функцию обработки изображения
    processed_image = await process_image_with_watermark(
        img_bytes.getvalue(),
        watermark_text,
        settings
    )
    
    return processed_image


async def send_test_preview(message: Message, token: str):
    """Отправляет тестовое изображение с текущими настройками"""
    try:
        watermark_text = slave_watermarks.get(token, "")
        settings = slave_watermark_settings.get(token, DEFAULT_WATERMARK_SETTINGS.copy())
        
        if not watermark_text:
            await message.answer("⚠️ Текст водяного знака не установлен. Установите текст перед просмотром.")
            return
        
        # Генерируем тестовое изображение
        test_image = await generate_test_image(watermark_text, settings)
        
        # Отправляем изображение
        input_file = BufferedInputFile(
            test_image,
            filename="watermark_preview.jpg"
        )
        
        await message.answer_photo(
            photo=input_file,
            caption="📸 Предпросмотр водяного знака на белом и черном фоне"
        )
    except Exception as e:
        logger.error(f"Ошибка генерации тестового изображения: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка генерации предпросмотра: {str(e)}")


async def process_image_with_watermark(
    image_bytes: bytes, 
    watermark_text: str, 
    settings: Optional[Dict] = None
) -> bytes:
    """Обрабатывает изображение: увеличивает разрешение x2 и добавляет водяной знак"""
    if settings is None:
        settings = DEFAULT_WATERMARK_SETTINGS.copy()
    
    logger.info(f"Начало обработки изображения. Размер: {len(image_bytes)} байт")
    logger.info(f"Водяной знак: {watermark_text}")
    logger.info(f"Настройки: {settings}")
    
    try:
        img = Image.open(BytesIO(image_bytes))
        logger.info(f"Исходное изображение: {img.size}, режим: {img.mode}, формат: {img.format}")
        
        # Если включен автоматический цвет, вычисляем яркость до увеличения размера (для оптимизации)
        auto_color = settings.get('auto_color', DEFAULT_WATERMARK_SETTINGS['auto_color'])
        auto_color_value = None
        if auto_color:
            # Сохраняем копию для вычисления яркости
            brightness_img = img.copy()
            auto_color_value = get_auto_color(brightness_img)
            logger.info(f"Автоматический цвет определен: RGB{auto_color_value}")
        
        # Увеличиваем разрешение в 2 раза используя NEAREST для pixel art
        # NEAREST сохраняет четкие пиксели без размытия
        new_size = (img.width * 2, img.height * 2)
        logger.info(f"Увеличение разрешения: {img.size} -> {new_size}")
        img = img.resize(new_size, Image.NEAREST)
        
        # Конвертируем в RGBA для прозрачности
        if img.mode != 'RGBA':
            logger.info(f"Конвертация из {img.mode} в RGBA")
            img = img.convert('RGBA')
        
        # Создаем слой для водяного знака
        watermark_layer = Image.new('RGBA', img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(watermark_layer)
        
        # Определяем размер шрифта из настроек
        min_side = min(img.width, img.height)
        size_percent = settings.get('size_percent', DEFAULT_WATERMARK_SETTINGS['size_percent'])
        target_text_size = int(min_side * size_percent)
        logger.info(f"Минимальная сторона: {min_side}px, целевой размер текста: {target_text_size}px ({size_percent*100:.0f}%)")
        
        # Загружаем шрифт Roboto.ttf
        font = None
        font_paths = [
            "Roboto.ttf",  # В корне проекта
            "./Roboto.ttf",
            "fonts/Roboto.ttf",
            "/usr/share/fonts/truetype/roboto/Roboto-Regular.ttf",  # Linux
            "/System/Library/Fonts/Supplemental/Arial.ttf"  # macOS fallback
        ]
        
        # Подбираем размер шрифта, чтобы текст занимал примерно 30% от минимальной стороны
        font_size = target_text_size
        for font_path in font_paths:
            try:
                # Начинаем с целевого размера и подбираем оптимальный
                for size in range(font_size, 10, -5):
                    test_font = ImageFont.truetype(font_path, size)
                    bbox = draw.textbbox((0, 0), watermark_text, font=test_font)
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                    
                    # Проверяем, что текст не больше целевого размера
                    if max(text_width, text_height) <= target_text_size:
                        font = test_font
                        font_size = size
                        logger.info(f"Загружен шрифт: {font_path}, размер: {font_size}px")
                        logger.info(f"Размер текста: {text_width}x{text_height}px")
                        break
                
                if font:
                    break
            except Exception as e:
                logger.debug(f"Не удалось загрузить шрифт {font_path}: {e}")
                continue
        
        # Если не удалось загрузить ни один шрифт, используем стандартный
        if not font:
            font = ImageFont.load_default()
            logger.warning("Использован стандартный шрифт (Roboto.ttf не найден)")
        
        # Вычисляем финальный размер текста
        bbox = draw.textbbox((0, 0), watermark_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Позиция: справа снизу с отступом (5% от размера изображения)
        margin_x = int(img.width * 0.05)
        margin_y = int(img.height * 0.05)
        x = img.width - text_width - margin_x
        y = img.height - text_height - margin_y
        logger.info(f"Позиция водяного знака: ({x}, {y}), отступы: ({margin_x}, {margin_y})")
        
        # Получаем цвет и прозрачность из настроек
        opacity = settings.get('opacity', DEFAULT_WATERMARK_SETTINGS['opacity'])
        stroke_enabled = settings.get('stroke_enabled', DEFAULT_WATERMARK_SETTINGS['stroke_enabled'])
        stroke_width = settings.get('stroke_width', DEFAULT_WATERMARK_SETTINGS['stroke_width'])
        
        # Определяем цвет: автоматический или из настроек
        if auto_color and auto_color_value:
            # Используем предварительно вычисленный автоматический цвет
            color_r, color_g, color_b = auto_color_value
            logger.info(f"Автоматический цвет: RGB({color_r}, {color_g}, {color_b})")
        else:
            # Используем цвет из настроек
            color_r = settings.get('color_r', DEFAULT_WATERMARK_SETTINGS['color_r'])
            color_g = settings.get('color_g', DEFAULT_WATERMARK_SETTINGS['color_g'])
            color_b = settings.get('color_b', DEFAULT_WATERMARK_SETTINGS['color_b'])
            logger.info(f"Цвет из настроек: RGB({color_r}, {color_g}, {color_b})")
        
        # Определяем цвет обводки
        stroke_fill = None
        if stroke_enabled:
            if auto_color and auto_color_value:
                # При автоматическом цвете обводка - инвертированный цвет текста
                stroke_r, stroke_g, stroke_b = invert_color((color_r, color_g, color_b))
                stroke_fill = (stroke_r, stroke_g, stroke_b, opacity)
                logger.info(f"Автоматическая обводка (инвертированный цвет): RGB({stroke_r}, {stroke_g}, {stroke_b})")
            else:
                # При ручном цвете обводка - инвертированный цвет текста
                stroke_r, stroke_g, stroke_b = invert_color((color_r, color_g, color_b))
                stroke_fill = (stroke_r, stroke_g, stroke_b, opacity)
                logger.info(f"Обводка (инвертированный цвет): RGB({stroke_r}, {stroke_g}, {stroke_b})")
        
        # Рисуем текст с настройками цвета, прозрачности и обводки
        if stroke_enabled and stroke_fill:
            draw.text(
                (x, y), 
                watermark_text, 
                fill=(color_r, color_g, color_b, opacity),
                font=font,
                stroke_width=stroke_width,
                stroke_fill=stroke_fill
            )
            logger.info(f"Текст с обводкой: цвет RGB({color_r}, {color_g}, {color_b}), обводка RGB{stroke_fill[:3]}, толщина {stroke_width}px")
        else:
            draw.text((x, y), watermark_text, fill=(color_r, color_g, color_b, opacity), font=font)
            logger.info(f"Текст без обводки: цвет RGB({color_r}, {color_g}, {color_b}), прозрачность {opacity}/255")
        
        # Накладываем водяной знак
        img = Image.alpha_composite(img, watermark_layer)
        
        # Конвертируем обратно в RGB для сохранения в JPEG
        img = img.convert('RGB')
        logger.info("Конвертация в RGB для сохранения")
        
        # Сохраняем в BytesIO
        output = BytesIO()
        img.save(output, format='JPEG', quality=95)
        output.seek(0)
        
        result_bytes = output.getvalue()
        logger.info(f"Обработка завершена. Размер результата: {len(result_bytes)} байт")
        
        return result_bytes
    except Exception as e:
        logger.error(f"Ошибка при обработке изображения: {e}", exc_info=True)
        raise



async def process_zip_archive(
    zip_bytes: bytes, 
    watermark_text: str, 
    settings: Optional[Dict] = None
) -> bytes:
    """Обрабатывает ZIP архив: извлекает, обрабатывает изображения и упаковывает обратно"""
    if settings is None:
        settings = DEFAULT_WATERMARK_SETTINGS.copy()
    
    logger.info(f"Начало обработки ZIP архива. Размер: {len(zip_bytes)} байт")
    
    # Создаем временные директории для распаковки и упаковки
    with tempfile.TemporaryDirectory() as temp_in, tempfile.TemporaryDirectory() as temp_out:
        # Сохраняем входящий zip
        zip_path = Path(temp_in) / "input.zip"
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)
            
        # Распаковываем
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_in)
        except zipfile.BadZipFile:
            logger.error("Некорректный ZIP файл")
            raise ValueError("Файл поврежден или не является ZIP архивом")
            
        # Удаляем исходный zip, чтобы не обрабатывать его
        os.remove(zip_path)
        
        # Итерируемся по файлам
        input_path = Path(temp_in)
        output_path = Path(temp_out)
        
        image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.webp', '.tiff'}
        processed_count = 0
        
        for root, dirs, files in os.walk(input_path):
            # Создаем структуру папок в выходной директории
            rel_path = Path(root).relative_to(input_path)
            current_out_dir = output_path / rel_path
            current_out_dir.mkdir(parents=True, exist_ok=True)
            
            for file in files:
                file_path = Path(root) / file
                # Игнорируем скрытые файлы (например __MACOSX)
                if file.startswith('.'):
                    continue
                    
                file_ext = file_path.suffix.lower()
                

                out_file_path = current_out_dir / file
                
                if file_ext in image_extensions:
                    try:
                        # Читаем изображение
                        with open(file_path, "rb") as f:
                            img_data = f.read()
                        
                        # Обрабатываем
                        processed_data = await process_image_with_watermark(
                            img_data, 
                            watermark_text, 
                            settings
                        )
                        
                        # Сохраняем обработанное с префиксом watermarked_
                        out_processed_path = current_out_dir / f"watermarked_{file}"
                        with open(out_processed_path, "wb") as f:
                            f.write(processed_data)
                            
                        processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Не удалось обработать изображение {file}: {e}")
                        # Если ошибка, копируем оригинал с исходным именем
                        shutil.copy2(file_path, out_file_path)
                else:
                    # Копируем остальные файлы без изменений
                    shutil.copy2(file_path, out_file_path)
        
        logger.info(f"Обработано изображений в архиве: {processed_count}")
        
        # Создаем новый архив
        archive_base = str(Path(tempfile.gettempdir()) / f"processed_{os.urandom(8).hex()}")
        shutil.make_archive(archive_base, 'zip', temp_out)
        
        archive_path = archive_base + ".zip"
        
        # Читаем результат
        with open(archive_path, "rb") as f:
            result_bytes = f.read()
            
        # Удаляем временный архив
        os.remove(archive_path)
        
        return result_bytes


def create_slave_router(watermark_text: str, token: Optional[str] = None) -> Router:
    """Создает router для slave бота с заданным водяным знаком"""
    router = Router()
    
    @router.message(CommandStart())
    async def slave_start(message: Message):
        await message.answer(

            f"👋 Привет! Я slave бот для добавления водяных знаков.\n\n"
            f"💧 Мой водяной знак: {watermark_text}\n\n"
            f"📤 Отправь мне изображение как файл или ZIP архив, и я:\n"
            f"1️⃣ Распакую архив (если отправлен архив)\n"
            f"2️⃣ Увеличу разрешение всех картинок в 2 раза\n"
            f"3️⃣ Добавлю водяной знак\n"
            f"4️⃣ Отправлю готовый файл или архив обратно с сохранением структуры папок"
        )
    
    @router.message(F.document)
    async def handle_document(message: Message):
        doc = message.document
        logger.info(f"Slave bot: получен документ от {message.from_user.id}")
        logger.info(f"Тип файла: {doc.mime_type}, размер: {doc.file_size}, имя: {doc.file_name}")
        

        # Определяем тип файла
        is_image = doc.mime_type and doc.mime_type.startswith('image/')
        is_zip = doc.mime_type in ('application/zip', 'application/x-zip-compressed') or (doc.file_name and doc.file_name.lower().endswith('.zip'))

        if not is_image and not is_zip:
            logger.warning(f"Получен неподдерживаемый файл: {doc.mime_type}")
            await message.answer("❌ Пожалуйста, отправьте файл изображения или ZIP архив с изображениями")
            return
        
        status_text = "⏳ Обрабатываю архив..." if is_zip else "⏳ Обрабатываю изображение..."
        processing_message = await message.answer(status_text)
        
        try:
            # Получаем файл
            logger.info(f"Скачивание файла {doc.file_id}...")
            file = await message.bot.get_file(doc.file_id)
            logger.info(f"Путь к файлу: {file.file_path}")
            
            file_bytes_io = await message.bot.download_file(file.file_path)
            file_data = file_bytes_io.read()
            logger.info(f"Файл скачан, размер: {len(file_data)} байт")
            
            # Получаем настройки для этого slave бота
            settings = slave_watermark_settings.get(token, DEFAULT_WATERMARK_SETTINGS.copy()) if token else DEFAULT_WATERMARK_SETTINGS.copy()
            
            if is_zip:
                logger.info("Начало обработки ZIP архива...")
                processed_data = await process_zip_archive(file_data, watermark_text, settings)
                output_filename = f"watermarked_{doc.file_name}"
            else:
                logger.info("Начало обработки изображения...")
                processed_data = await process_image_with_watermark(file_data, watermark_text, settings)            
                output_filename = f"watermarked_{doc.file_name}"
            
            # Отправляем обработанный файл
            logger.info("Отправка обработанного файла...")
            input_file = BufferedInputFile(
                processed_data,
                filename=output_filename
            )
            
            # Удаляем сообщение о статусе
            if processing_message:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=processing_message.message_id)
                logger.info("Сообщение о статусе удалено")

            await message.answer_document(document=input_file)
            logger.info("Файл отправлен пользователю")
            
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}", exc_info=True)
            text_error = "архива" if is_zip else "изображения"
            await message.answer(f"❌ Ошибка обработки {text_error}: {str(e)}")
    
    @router.message(F.photo)
    async def handle_photo(message: Message):
        logger.info(f"Slave bot: получено фото (сжатое) от {message.from_user.id}")
        await message.answer(

            "⚠️ Пожалуйста, отправьте изображение как ФАЙЛ (не как фото) или ZIP архив,\n"
            "чтобы сохранить исходное качество и структуру.\n\n"
            "📎 Нажмите на скрепку → Файл → выберите изображение или архив"
        )
    
    return router


async def start_slave_bot(token: str, watermark_text: str, save_to_db: bool = True):
    """Запускает slave бота с заданным токеном и водяным знаком"""
    logger.info(f"Запуск slave бота с водяным знаком: {watermark_text}")
    
    bot = Bot(token=token)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Создаем и регистрируем router для этого slave бота
    router = create_slave_router(watermark_text, token)
    dp.include_router(router)
    
    # Сохраняем в глобальном хранилище
    active_slaves[token] = bot
    slave_watermarks[token] = watermark_text
    slave_dispatchers[token] = dp
    
    logger.info(f"Slave бот успешно настроен. Всего активных: {len(active_slaves)}")
    
    # Сохраняем в базу данных
    if save_to_db:
        save_slaves_to_db()
        logger.info("Slave бот сохранен в базу данных")
    
    # Запускаем polling в отдельной задаче с обработкой отмены
    task = asyncio.create_task(dp.start_polling(bot, handle_signals=False))
    return task


# ============= MAIN =============
async def main():
    if not MASTER_TOKEN:
        raise ValueError("MASTER_BOT_TOKEN не установлен в .env")
    if not MASTER_PASSWORD:
        raise ValueError("MASTER_PASSWORD не установлен в .env")
    
    logger.info("=" * 50)
    logger.info("Запуск Master Bot системы")
    logger.info("=" * 50)
    
    # Создаем master бота
    master_bot = Bot(token=MASTER_TOKEN)
    master_storage = MemoryStorage()
    master_dp = Dispatcher(storage=master_storage)
    
    # Регистрируем router для master бота
    master_dp.include_router(master_router)
    
    bot_info = await master_bot.get_me()
    logger.info(f"🚀 Master бот запущен: @{bot_info.username}")
    logger.info(f"🔐 Пароль для доступа установлен")
    logger.info(f"📊 Уровень логирования: INFO")
    
    # Восстанавливаем slave ботов из базы данных
    logger.info("📂 Восстановление slave ботов из базы данных...")
    await restore_slaves_from_db()
    
    print("\n" + "=" * 50)
    print(f"✅ Master бот успешно запущен: @{bot_info.username}")
    print(f"🔐 Используйте пароль из .env для авторизации")
    print(f"💾 Восстановлено slave ботов: {len(active_slaves)}")
    print(f"📁 База данных: {SLAVES_DB_FILE}")
    print("=" * 50 + "\n")
    
    # Запускаем master бота
    try:
        await master_dp.start_polling(master_bot, handle_signals=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Получен сигнал остановки")
    finally:
        logger.info("Начало процедуры завершения работы...")
        
        # Отменяем все задачи slave ботов
        logger.info(f"Отмена {len(slave_tasks)} задач slave ботов...")
        for task in slave_tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения всех задач с таймаутом
        if slave_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*slave_tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning("Таймаут при ожидании завершения задач")
        
        # Закрываем соединения ботов
        logger.info("Закрытие соединений ботов...")
        await master_bot.session.close()
        
        for token, slave_bot in list(active_slaves.items()):
            try:
                await slave_bot.session.close()
            except Exception as e:
                logger.error(f"Ошибка при закрытии slave бота: {e}")
        
        logger.info("✅ Все соединения закрыты. Работа завершена.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)