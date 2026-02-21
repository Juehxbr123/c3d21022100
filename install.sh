#!/bin/bash

# Цвета для красоты
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}=== УСТАНОВЩИК ТОКАРНОГО БОТА ===${NC}"

# 1. Проверка Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 не найден! Установите его (sudo apt install python3 python3-venv python3-pip)${NC}"
    exit 1
fi

# 2. Создание виртуального окружения
echo -e "${GREEN}[1/5] Создаем виртуальное окружение...${NC}"
python3 -m venv venv
source venv/bin/activate

# 3. Установка библиотек
echo -e "${GREEN}[2/5] Устанавливаем библиотеки...${NC}"
pip install -r requirements.txt
pip install cryptography # Иногда нужно для MySQL

# 4. Настройка .env и БД
echo -e "${GREEN}[3/5] Настройка конфигурации...${NC}"

if [ -f .env ]; then
    echo "Файл .env уже существует. Пропускаем создание."
else
    echo "Введите данные для настройки:"
    read -p "Токен бота (от BotFather): " BOT_TOKEN
    read -p "Хост БД (обычно 127.0.0.1): " DB_HOST
    read -p "Имя БД (создайте её заранее, например turner_db): " DB_NAME
    read -p "Пользователь БД: " DB_USER
    read -s -p "Пароль БД: " DB_PASS
    echo ""
    read -p "Придумайте пароль для Админки: " ADMIN_PASS
    read -p "Придумайте цифровой пароль для бота (/iamadmin ...): " BOT_PASS

    # Запись в .env
    cat > .env <<EOL
BOT_TOKEN=$BOT_TOKEN
DB_HOST=$DB_HOST
DB_USER=$DB_USER
DB_PASS=$DB_PASS
DB_NAME=$DB_NAME
ADMIN_PANEL_PASSWORD=$ADMIN_PASS
BOT_ADMIN_PASSWORD=$BOT_PASS
EOL

    # Создание конфига для PHP
    cat > php_config.php <<EOL
<?php
\$db_host = '$DB_HOST';
\$db_user = '$DB_USER';
\$db_pass = '$DB_PASS';
\$db_name = '$DB_NAME';
\$admin_pass = '$ADMIN_PASS';
?>
EOL
fi

# 5. Импорт таблиц в БД
echo -e "${GREEN}[4/5] Инициализация Базы Данных...${NC}"
# Небольшой питон-скрипт для заливки SQL, чтобы не требовать mysql-client
python3 - <<END_PYTHON
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor
    )
    with conn.cursor() as cursor:
        with open('schema.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
            # Разбиваем SQL на команды по точке с запятой
            statements = sql_script.split(';')
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
    conn.commit()
    conn.close()
    print("✅ Таблицы успешно созданы!")
except Exception as e:
    print(f"❌ Ошибка БД: {e}")
    exit(1)
END_PYTHON

if [ $? -ne 0 ]; then
    echo -e "${RED}Ошибка при работе с БД. Проверьте логин/пароль и существует ли база.${NC}"
    exit 1
fi

# 6. Создание сервиса Systemd
echo -e "${GREEN}[5/5] Настройка автозапуска (Systemd)...${NC}"
SERVICE_NAME="turner_bot"
CURRENT_DIR=$(pwd)
USER_NAME=$(whoami)

# Генерируем файл сервиса
sudo bash -c "cat > /etc/systemd/system/$SERVICE_NAME.service" <<EOL
[Unit]
Description=Turner Telegram Bot
After=network.target mysql.service

[Service]
User=$USER_NAME
WorkingDirectory=$CURRENT_DIR
ExecStart=$CURRENT_DIR/venv/bin/python $CURRENT_DIR/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOL

echo "Обновляем демоны..."
sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl start $SERVICE_NAME

echo -e "${CYAN}===========================================${NC}"
echo -e "${GREEN}ГОТОВО! Бот установлен и запущен.${NC}"
echo -e "Админка (PHP): скопируйте admin.php и php_config.php в папку вашего веб-сервера (напр. /var/www/html)"
echo -e "Проверка статуса: sudo systemctl status $SERVICE_NAME"
echo -e "${CYAN}===========================================${NC}"