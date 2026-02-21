# Author: Sergey Akulov
# GitHub: https://github.com/serg-akulov

import streamlit as st
import pandas as pd
import database
import config
import os

# Авторизация (простейшая)
if 'auth' not in st.session_state:
    st.session_state.auth = False

def login():
    st.title("Вход в систему Metalok")
    pwd = st.text_input("Пароль", type="password")
    if st.button("Войти"):
        if pwd == config.ADMIN_PANEL_PASSWORD:
            st.session_state.auth = True
            st.rerun()
        else:
            st.error("Неверный пароль")

if not st.session_state.auth:
    login()
    st.stop()

# --- Основной интерфейс ---
st.title("🛠 Управление заказами")

tab1, tab2 = st.tabs(["📋 Заказы", "⚙️ Настройки"])

with tab1:
    st.header("Список заявок")
    
    conn = database.get_connection()
    df = pd.read_sql("SELECT * FROM orders ORDER BY id DESC", conn)
    conn.close()
    
    st.dataframe(df)
    
    st.write("Чтобы изменить статус заказа, используй Telegram (просто ответь клиенту) или правь базу через Adminer, если нужно сложное редактирование.")

with tab2:
    st.header("Настройки бота")
    
    welcome_old = database.get_setting("welcome_text")
    welcome_new = st.text_area("Приветственное сообщение", value=welcome_old)
    
    if st.button("Сохранить настройки"):
        database.update_setting("welcome_text", welcome_new)
        st.success("Сохранено!")
    
    st.divider()
    st.subheader("Управление процессом")
    if st.button("🔴 ПЕРЕЗАПУСТИТЬ БОТА (Restart Service)"):
        # Это сработает, если бот запущен через systemd с именем turner_bot
        os.system("sudo systemctl restart turner_bot") 
        st.warning("Команда на перезапуск отправлена.")