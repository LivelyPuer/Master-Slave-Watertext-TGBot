#!/bin/bash

# ============================================
# Скрипт автозапуска и автообновления бота
# ============================================

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Конфигурация
REPO_URL="https://github.com/LivelyPuer/Master-Slave-Watertext-TGBot.git"
PROJECT_DIR="$HOME/Master-Slave-Watertext-TGBot"
VENV_DIR="$PROJECT_DIR/.venv"
MAIN_SCRIPT="$PROJECT_DIR/main.py"
LOG_FILE="$PROJECT_DIR/bot.log"
PID_FILE="$PROJECT_DIR/bot.pid"

# Функция вывода с цветом
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Функция проверки команды
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Проверка зависимостей
check_dependencies() {
    print_info "Проверка зависимостей..."
    
    local missing_deps=()
    
    if ! command_exists git; then
        missing_deps+=("git")
    fi
    
    if ! command_exists python3; then
        missing_deps+=("python3")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Отсутствуют необходимые зависимости: ${missing_deps[*]}"
        print_info "Установите их с помощью:"
        print_info "  Ubuntu/Debian: sudo apt-get install ${missing_deps[*]}"
        print_info "  macOS: brew install ${missing_deps[*]}"
        exit 1
    fi
    
    print_success "Все зависимости установлены"
}

# Клонирование или обновление репозитория
setup_repository() {
    if [ -d "$PROJECT_DIR" ]; then
        print_info "Директория проекта существует, обновляем..."
        cd "$PROJECT_DIR" || exit 1
        
        # Сохраняем локальные изменения
        git stash > /dev/null 2>&1
        
        # Получаем последние изменения
        git fetch origin
        
        # Проверяем, есть ли обновления
        LOCAL=$(git rev-parse @)
        REMOTE=$(git rev-parse @{u})
        
        if [ "$LOCAL" = "$REMOTE" ]; then
            print_info "Репозиторий уже обновлен"
            return 0
        else
            print_info "Найдены обновления, выполняю git pull..."
            if git pull origin main; then
                print_success "Репозиторий успешно обновлен"
                
                # Восстанавливаем локальные изменения
                git stash pop > /dev/null 2>&1
                
                return 1  # Возвращаем 1, чтобы показать что было обновление
            else
                print_error "Ошибка при обновлении репозитория"
                git stash pop > /dev/null 2>&1
                exit 1
            fi
        fi
    else
        print_info "Клонирование репозитория..."
        if git clone "$REPO_URL" "$PROJECT_DIR"; then
            print_success "Репозиторий успешно склонирован"
            cd "$PROJECT_DIR" || exit 1
            return 1  # Возвращаем 1, чтобы показать что был clone
        else
            print_error "Ошибка при клонировании репозитория"
            exit 1
        fi
    fi
}

# Настройка виртуального окружения
setup_venv() {
    print_info "Настройка виртуального окружения..."
    
    if [ ! -d "$VENV_DIR" ]; then
        print_info "Создание виртуального окружения..."
        python3 -m venv "$VENV_DIR"
        print_success "Виртуальное окружение создано"
    fi
    
    # Активация виртуального окружения
    source "$VENV_DIR/bin/activate"
    
    # Обновление pip
    print_info "Обновление pip..."
    pip install --upgrade pip > /dev/null 2>&1
    
    # Установка зависимостей
    if [ -f "$PROJECT_DIR/requirements.txt" ]; then
        print_info "Установка зависимостей..."
        if pip install -r "$PROJECT_DIR/requirements.txt" > /dev/null 2>&1; then
            print_success "Зависимости установлены"
        else
            print_warning "Некоторые зависимости могут быть не установлены"
        fi
    fi
}

# Проверка .env файла
check_env() {
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        print_error "Файл .env не найден!"
        print_info "Создайте файл .env со следующим содержимым:"
        echo ""
        echo "MASTER_BOT_TOKEN=your_master_bot_token_here"
        echo "MASTER_PASSWORD=your_secure_password_here"
        echo ""
        print_info "Затем запустите скрипт снова"
        exit 1
    fi
    print_success "Файл .env найден"
}

# Проверка шрифта Roboto
check_font() {
    if [ ! -f "$PROJECT_DIR/Roboto.ttf" ]; then
        print_warning "Файл Roboto.ttf не найден"
        print_info "Скачиваю шрифт Roboto..."
        
        if command_exists curl; then
            curl -L -o "$PROJECT_DIR/Roboto.ttf" \
                "https://github.com/google/roboto/raw/main/src/hinted/Roboto-Regular.ttf" \
                > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                print_success "Шрифт Roboto.ttf скачан"
            else
                print_warning "Не удалось скачать шрифт, будет использован системный"
            fi
        elif command_exists wget; then
            wget -O "$PROJECT_DIR/Roboto.ttf" \
                "https://github.com/google/roboto/raw/main/src/hinted/Roboto-Regular.ttf" \
                > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                print_success "Шрифт Roboto.ttf скачан"
            else
                print_warning "Не удалось скачать шрифт, будет использован системный"
            fi
        else
            print_warning "curl или wget не найдены, скачайте Roboto.ttf вручную"
        fi
    else
        print_success "Файл Roboto.ttf найден"
    fi
}

# Остановка бота
stop_bot() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            print_info "Остановка бота (PID: $PID)..."
            kill "$PID" 2>/dev/null
            
            # Ждем завершения процесса
            local count=0
            while ps -p "$PID" > /dev/null 2>&1 && [ $count -lt 10 ]; do
                sleep 0.5
                count=$((count + 1))
            done
            
            # Если процесс все еще работает, принудительно завершаем
            if ps -p "$PID" > /dev/null 2>&1; then
                print_warning "Принудительная остановка бота..."
                kill -9 "$PID" 2>/dev/null
            fi
            
            rm -f "$PID_FILE"
            print_success "Бот остановлен"
        else
            rm -f "$PID_FILE"
        fi
    fi
}

# Запуск бота
start_bot() {
    print_info "Запуск бота..."
    
    cd "$PROJECT_DIR" || exit 1
    source "$VENV_DIR/bin/activate"
    
    # Запуск бота в фоновом режиме
    nohup python3 "$MAIN_SCRIPT" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    
    # Проверка запуска
    sleep 2
    if ps -p $(cat "$PID_FILE") > /dev/null 2>&1; then
        print_success "Бот успешно запущен (PID: $(cat "$PID_FILE"))"
        print_info "Логи: tail -f $LOG_FILE"
    else
        print_error "Не удалось запустить бота, проверьте логи: $LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Перезапуск бота
restart_bot() {
    print_info "Перезапуск бота..."
    stop_bot
    sleep 1
    start_bot
}

# Показать статус
show_status() {
    echo ""
    print_info "========================================"
    print_info "  Статус бота"
    print_info "========================================"
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            print_success "Бот работает (PID: $PID)"
            print_info "Проект: $PROJECT_DIR"
            print_info "Логи: $LOG_FILE"
            
            if [ -f "$PROJECT_DIR/slaves_database.json" ]; then
                SLAVES_COUNT=$(python3 -c "import json; print(len(json.load(open('$PROJECT_DIR/slaves_database.json'))))" 2>/dev/null)
                print_info "Slave ботов в базе: $SLAVES_COUNT"
            fi
        else
            print_warning "Бот не работает (устаревший PID файл)"
            rm -f "$PID_FILE"
        fi
    else
        print_warning "Бот не запущен"
    fi
    
    echo ""
}

# Показать логи
show_logs() {
    if [ -f "$LOG_FILE" ]; then
        print_info "Последние 50 строк логов:"
        echo ""
        tail -n 50 "$LOG_FILE"
        echo ""
        print_info "Для просмотра в реальном времени: tail -f $LOG_FILE"
    else
        print_warning "Файл логов не найден"
    fi
}

# Главная функция
main() {
    echo ""
    print_info "========================================"
    print_info "  Master-Slave Watermark Bot Manager"
    print_info "========================================"
    echo ""
    
    # Обработка аргументов
    case "${1:-}" in
        stop)
            stop_bot
            ;;
        start)
            check_dependencies
            setup_repository
            setup_venv
            check_env
            check_font
            start_bot
            ;;
        restart)
            check_dependencies
            setup_repository
            UPDATED=$?
            setup_venv
            check_env
            check_font
            restart_bot
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        update)
            check_dependencies
            stop_bot
            setup_repository
            setup_venv
            check_env
            check_font
            start_bot
            ;;
        *)
            # Режим по умолчанию: обновление и запуск
            check_dependencies
            setup_repository
            UPDATED=$?
            setup_venv
            check_env
            check_font
            
            # Если бот уже запущен и были обновления, перезапускаем
            if [ -f "$PID_FILE" ] && [ $UPDATED -eq 1 ]; then
                restart_bot
            elif [ -f "$PID_FILE" ]; then
                print_info "Бот уже запущен, обновлений нет"
                show_status
            else
                start_bot
            fi
            
            show_status
            ;;
    esac
}

# Запуск
main "$@"