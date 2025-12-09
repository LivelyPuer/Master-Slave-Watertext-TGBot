import asyncio
import os
import json
import logging
from io import BytesIO
from typing import Dict, Optional, List
from pathlib import Path

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
slave_dispatchers: Dict[str, Dispatcher] = {}
slave_tasks: List[asyncio.Task] = []  # Задачи polling для slave ботов


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
        slaves_data.append({
            "token": token,
            "watermark": slave_watermarks.get(token, "")
        })
    
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
        
        if not token:
            logger.warning("Пропущена запись без токена")
            continue
        
        try:
            logger.info(f"Восстановление slave бота с водяным знаком: {watermark}")
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


class SlaveStates(StatesGroup):
    processing_images = State()


# ============= MASTER BOT =============
master_router = Router()
authenticated_users = set()


@master_router.message(CommandStart())
async def master_start(message: Message, state: FSMContext):
    logger.info(f"Master bot: команда /start от пользователя {message.from_user.id}")
    if message.from_user.id in authenticated_users:
        await message.answer(
            "🤖 Вы уже авторизованы!\n\n"
            "Доступные команды:\n"
            "/create_slave - Создать нового slave бота\n"
            "/list_slaves - Список активных slave ботов\n"
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
    
    task = await start_slave_bot(token, watermark_text, save_to_db=True)
    slave_tasks.append(task)
    
    await message.answer(
        f"🎉 Slave бот успешно создан и запущен!\n\n"
        f"🤖 Бот: @{bot_username}\n"
        f"💧 Водяной знак: {watermark_text}\n\n"
        f"Теперь вы можете отправлять изображения этому боту.\n"
        f"💾 Данные сохранены в базу данных."
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


# ============= SLAVE BOT ЛОГИКА =============
async def process_image_with_watermark(image_bytes: bytes, watermark_text: str) -> bytes:
    """Обрабатывает изображение: увеличивает разрешение x2 и добавляет водяной знак"""
    logger.info(f"Начало обработки изображения. Размер: {len(image_bytes)} байт")
    logger.info(f"Водяной знак: {watermark_text}")
    
    try:
        img = Image.open(BytesIO(image_bytes))
        logger.info(f"Исходное изображение: {img.size}, режим: {img.mode}, формат: {img.format}")
        
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
        
        # Определяем размер шрифта: 30% от минимальной стороны
        min_side = min(img.width, img.height)
        target_text_size = int(min_side * 0.5)
        logger.info(f"Минимальная сторона: {min_side}px, целевой размер текста: {target_text_size}px")
        
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
        
        # Рисуем полупрозрачный текст (белый с 50% прозрачностью)
        draw.text((x, y), watermark_text, fill=(255, 255, 255, 128), font=font)
        
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


def create_slave_router(watermark_text: str) -> Router:
    """Создает router для slave бота с заданным водяным знаком"""
    router = Router()
    
    @router.message(CommandStart())
    async def slave_start(message: Message):
        await message.answer(
            f"👋 Привет! Я slave бот для добавления водяных знаков.\n\n"
            f"💧 Мой водяной знак: {watermark_text}\n\n"
            f"📤 Отправь мне изображение(я) как файл, и я:\n"
            f"1️⃣ Увеличу разрешение в 2 раза\n"
            f"2️⃣ Добавлю водяной знак\n"
            f"3️⃣ Отправлю обработанное изображение обратно"
        )
    
    @router.message(F.document)
    async def handle_document(message: Message):
        doc = message.document
        logger.info(f"Slave bot: получен документ от {message.from_user.id}")
        logger.info(f"Тип файла: {doc.mime_type}, размер: {doc.file_size}, имя: {doc.file_name}")
        
        # Проверяем, что это изображение
        if not doc.mime_type or not doc.mime_type.startswith('image/'):
            logger.warning(f"Получен не-изображение: {doc.mime_type}")
            await message.answer("❌ Пожалуйста, отправьте файл изображения (JPEG, PNG и т.д.)")
            return
        
        await message.answer("⏳ Обрабатываю изображение...")
        
        try:
            # Получаем файл
            logger.info(f"Скачивание файла {doc.file_id}...")
            file = await message.bot.get_file(doc.file_id)
            logger.info(f"Путь к файлу: {file.file_path}")
            
            file_bytes = await message.bot.download_file(file.file_path)
            logger.info(f"Файл скачан, размер: {len(file_bytes.getvalue())} байт")
            
            # Обрабатываем изображение
            logger.info("Начало обработки изображения...")
            processed_image = await process_image_with_watermark(
                file_bytes.read(), 
                watermark_text
            )            
            # Отправляем обработанное изображение используя BufferedInputFile
            logger.info("Отправка обработанного изображения...")
            input_file = BufferedInputFile(
                processed_image,
                filename=f"watermarked_{doc.file_name}"
            )
            
            await message.answer_document(
                document=input_file)
            logger.info("Изображение отправлено пользователю")
            
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}", exc_info=True)
            await message.answer(f"❌ Ошибка обработки: {str(e)}")
    
    @router.message(F.photo)
    async def handle_photo(message: Message):
        logger.info(f"Slave bot: получено фото (сжатое) от {message.from_user.id}")
        await message.answer(
            "⚠️ Пожалуйста, отправьте изображение как ФАЙЛ (не как фото),\n"
            "чтобы сохранить исходное качество.\n\n"
            "📎 Нажмите на скрепку → Файл → выберите изображение"
        )
    
    return router


async def start_slave_bot(token: str, watermark_text: str, save_to_db: bool = True):
    """Запускает slave бота с заданным токеном и водяным знаком"""
    logger.info(f"Запуск slave бота с водяным знаком: {watermark_text}")
    
    bot = Bot(token=token)
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Создаем и регистрируем router для этого slave бота
    router = create_slave_router(watermark_text)
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