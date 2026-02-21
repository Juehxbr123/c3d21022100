import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ContentType
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import database
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chel3d_bot")

UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)


def user_full_name(user: Any) -> str:
    first = getattr(user, "first_name", "") or ""
    last = getattr(user, "last_name", "") or ""
    name = (first + " " + last).strip()
    return name or getattr(user, "full_name", "") or "Без имени"


def user_username(user: Any) -> str | None:
    return getattr(user, "username", None)


def bot_cfg() -> dict[str, str]:
    try:
        return database.get_bot_config()
    except Exception:
        return {}


def get_cfg(key: str, default: str = "") -> str:
    val = bot_cfg().get(key, "")
    if val is None or val == "":
        return default
    return str(val)


def cfg_bool(key: str, default: bool = True) -> bool:
    raw = bot_cfg().get(key, "")
    if raw is None or raw == "":
        return default
    return str(raw).lower() in {"1", "true", "yes", "on"}


def photo_ref_for(step_key: str) -> str:
    cfg = bot_cfg()
    return (
        cfg.get(step_key, "")
        or cfg.get("placeholder_photo_path", "")
        or getattr(settings, "placeholder_photo_path", "")
    )


def get_orders_chat_id() -> str:
    return get_cfg("orders_chat_id", getattr(settings, "orders_chat_id", ""))


def normalize_chat_id(value: str) -> int | str:
    cleaned = (value or "").strip().replace(" ", "")
    if cleaned.startswith("-") and cleaned[1:].isdigit():
        return int(cleaned)
    if cleaned.isdigit():
        return int(cleaned)
    return cleaned


class Form(StatesGroup):
    step = State()


def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


def nav_row(include_back: bool = True) -> list[InlineKeyboardButton]:
    row: list[InlineKeyboardButton] = []
    if include_back:
        row.append(InlineKeyboardButton(text="🔙 Назад", callback_data="nav:back"))
    row.append(InlineKeyboardButton(text="🏠 Главное меню", callback_data="nav:menu"))
    return row


def menu_kb() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if cfg_bool("enabled_menu_print", True):
        rows.append([InlineKeyboardButton(text=get_cfg("btn_menu_print", "📐 Рассчитать печать"), callback_data="menu:print")])
    if cfg_bool("enabled_menu_scan", True):
        rows.append([InlineKeyboardButton(text=get_cfg("btn_menu_scan", "📡 3D-сканирование"), callback_data="menu:scan")])
    if cfg_bool("enabled_menu_idea", True):
        rows.append([InlineKeyboardButton(text=get_cfg("btn_menu_idea", "❓ Нет модели / Хочу придумать"), callback_data="menu:idea")])
    if cfg_bool("enabled_menu_about", True):
        rows.append([InlineKeyboardButton(text=get_cfg("btn_menu_about", "ℹ️ О нас"), callback_data="menu:about")])
    if not rows:
        rows = [[InlineKeyboardButton(text="ℹ️ О нас", callback_data="menu:about")]]
    return kb(rows)


def step_keyboard_for_print(payload: dict[str, Any]) -> InlineKeyboardMarkup:
    tech = payload.get("technology")
    if tech == "FDM":
        items = [
            ("btn_mat_petg", "PET-G"),
            ("btn_mat_pla", "PLA"),
            ("btn_mat_petg_carbon", "PET-G Carbon"),
            ("btn_mat_tpu", "TPU"),
            ("btn_mat_nylon", "Нейлон"),
            ("btn_mat_other", "🤔 Другой материал"),
        ]
    elif tech == "Фотополимер":
        items = [
            ("btn_resin_standard", "Стандартная"),
            ("btn_resin_abs", "ABS-Like"),
            ("btn_resin_tpu", "TPU-Like"),
            ("btn_resin_nylon", "Нейлон-Like"),
            ("btn_resin_other", "🤔 Другая смола"),
        ]
    else:
        items = [("", "Пропустить")]

    rows: list[list[InlineKeyboardButton]] = []
    for key, label in items:
        txt = get_cfg(key, label) if key else label
        rows.append([InlineKeyboardButton(text=txt, callback_data=f"set:material:{label}")])
    rows.append(nav_row())
    return kb(rows)


async def send_step(
    message: Message,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
    photo_ref: Optional[str] = None,
) -> Message:
    ref = photo_ref or getattr(settings, "placeholder_photo_path", "")
    if ref:
        try:
            if ref.startswith("http://") or ref.startswith("https://"):
                return await message.answer_photo(photo=ref, caption=text, reply_markup=keyboard)

            p = Path(ref)
            if p.exists() and p.is_file():
                return await message.answer_photo(photo=FSInputFile(str(p)), caption=text, reply_markup=keyboard)

            return await message.answer_photo(photo=ref, caption=text, reply_markup=keyboard)
        except Exception:
            logger.exception("Не удалось отправить фото — отправляю текстом")

    return await message.answer(text, reply_markup=keyboard)


async def send_step_cb(
    cb: CallbackQuery,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
    photo_ref: Optional[str] = None,
) -> None:
    """Send a step message and safely acknowledge callback.

    NOTE: We sometimes call render_step() from non-callback contexts by creating a fake CallbackQuery.
    Such objects are not 'mounted' to a Bot instance, so cb.answer() raises RuntimeError in aiogram v3.
    """
    if cb.message:
        await send_step(cb.message, text, keyboard, photo_ref)

    # Acknowledge callback only if possible (real callback query). For fake callbacks, just ignore.
    try:
        await cb.answer()
    except RuntimeError:
        # Fallback: answer via bot instance if available
        try:
            if cb.message and getattr(cb.message, 'bot', None) and getattr(cb, 'id', None):
                await cb.message.bot.answer_callback_query(cb.id)
        except Exception:
            pass
    except Exception:
        pass


def payload_summary(payload: dict[str, Any]) -> str:
    branch_map = {"print": "Рассчитать печать", "scan": "3D-сканирование", "idea": "Нет модели / Хочу придумать", "dialog": "Диалог"}
    field_map = {
        "technology": "Технология",
        "material": "Материал",
        "material_custom": "Свой материал",
        "scan_type": "Тип сканирования",
        "idea_type": "Категория",
        "description": "Описание",
        "file": "Файл",
    }
    branch = str(payload.get("branch", ""))
    parts: list[str] = [f"Тип заявки: {branch_map.get(branch, branch)}"]
    for k, v in payload.items():
        if k == "branch" or v in (None, ""):
            continue
        parts.append(f"• {field_map.get(k, k)}: {v}")
    return "\n".join(parts)


def review_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="➕ Добавить описание", callback_data="review:add_description")],
        [InlineKeyboardButton(text="✅ Отправить заявку", callback_data="review:send")],
        nav_row(),
    ]
    return kb(rows)


async def persist(state: FSMContext) -> None:
    data = await state.get_data()
    order_id = data.get("order_id")
    if not order_id:
        return
    payload = data.get("payload", {})
    database.update_order_payload(int(order_id), payload, payload_summary(payload))


def _push_history(state_data: dict[str, Any]) -> list[str]:
    history: list[str] = state_data.get("history", [])
    current = state_data.get("current_step")
    if current:
        history.append(current)
    return history


async def show_main(message: Message, state: FSMContext) -> None:
    await state.clear()
    await send_step(
        message,
        get_cfg("welcome_menu_msg", "Привет! 👋 Я бот Chel3D.\nВыберите, что вам нужно — и я соберу заявку по шагам."),
        menu_kb(),
        photo_ref_for("photo_main_menu"),
    )


async def start_order(cb: CallbackQuery, state: FSMContext, branch: str) -> None:
    order_id = database.create_order(cb.from_user.id, user_username(cb.from_user), user_full_name(cb.from_user), branch)
    await state.set_state(Form.step)
    await state.update_data(order_id=order_id, payload={"branch": branch}, history=[], current_step=None, waiting_text=None)


async def render_step(cb: CallbackQuery, state: FSMContext, step: str, from_back: bool = False) -> None:
    if not from_back:
        data = await state.get_data()
        await state.update_data(history=_push_history(data))
    await state.update_data(current_step=step, waiting_text=None)

    data = await state.get_data()
    payload: dict[str, Any] = data.get("payload", {})

    if step == "print_tech":
        rows: list[list[InlineKeyboardButton]] = []
        if cfg_bool("enabled_print_fdm", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_print_fdm", "🧵 FDM (Пластик)"), callback_data="set:technology:FDM")])
        if cfg_bool("enabled_print_resin", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_print_resin", "💧 Фотополимер"), callback_data="set:technology:Фотополимер")])
        if cfg_bool("enabled_print_unknown", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_print_unknown", "🤷 Не знаю"), callback_data="set:technology:Не знаю")])
        rows.append(nav_row(False))
        await send_step_cb(cb, get_cfg("text_print_tech", "🖨 Выберите технологию печати:"), kb(rows), photo_ref_for("photo_print"))
        return

    if step == "print_material":
        await send_step_cb(cb, get_cfg("text_select_material", "Выберите материал:"), step_keyboard_for_print(payload), photo_ref_for("photo_print"))
        return

    if step == "print_material_custom":
        await state.update_data(waiting_text="material_custom")
        await send_step_cb(cb, get_cfg("text_describe_material", "Опишите материал/смолу свободным текстом:"), kb([nav_row()]), photo_ref_for("photo_print"))
        return

    if step == "attach_file":
        rows = [[InlineKeyboardButton(text="❌ У меня нет файла", callback_data="set:file:нет")], nav_row()]
        await send_step_cb(cb, get_cfg("text_attach_file", "Прикрепите STL/3MF/OBJ или фото. Или нажмите кнопку ниже:"), kb(rows))
        return

    if step == "description":
        await state.update_data(waiting_text="description")
        await send_step_cb(cb, get_cfg("text_describe_task", "Опишите задачу, размеры, сроки и важные детали:"), kb([nav_row()]))
        return

    if step == "review":
        summary = payload_summary(payload)
        await send_step_cb(
            cb,
            f"Проверьте заявку и отправьте её менеджеру:\n\n{summary}",
            review_keyboard(),
        )
        return

    if step == "scan_type":
        rows: list[list[InlineKeyboardButton]] = []
        if cfg_bool("enabled_scan_human", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_scan_human", "🧑 Человек"), callback_data="set:scan_type:Человек")])
        if cfg_bool("enabled_scan_object", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_scan_object", "📦 Предмет"), callback_data="set:scan_type:Предмет")])
        if cfg_bool("enabled_scan_industrial", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_scan_industrial", "🏭 Промышленный объект"), callback_data="set:scan_type:Промышленный объект")])
        if cfg_bool("enabled_scan_other", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_scan_other", "🤔 Другое"), callback_data="set:scan_type:Другое")])
        rows.append(nav_row(False))
        await send_step_cb(cb, get_cfg("text_scan_type", "📡 Выберите тип объекта для 3D-сканирования:"), kb(rows), photo_ref_for("photo_scan"))
        return

    if step == "idea_type":
        rows: list[list[InlineKeyboardButton]] = []
        if cfg_bool("enabled_idea_photo", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_idea_photo", "✏️ По фото/эскизу"), callback_data="set:idea_type:По фото/эскизу")])
        if cfg_bool("enabled_idea_award", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_idea_award", "🏆 Сувенир/Кубок/Медаль"), callback_data="set:idea_type:Сувенир/Кубок/Медаль")])
        if cfg_bool("enabled_idea_master", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_idea_master", "📏 Мастер-модель"), callback_data="set:idea_type:Мастер-модель")])
        if cfg_bool("enabled_idea_sign", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_idea_sign", "🎨 Вывески"), callback_data="set:idea_type:Вывески")])
        if cfg_bool("enabled_idea_other", True):
            rows.append([InlineKeyboardButton(text=get_cfg("btn_idea_other", "🤔 Другое"), callback_data="set:idea_type:Другое")])
        rows.append(nav_row(False))
        await send_step_cb(cb, get_cfg("text_idea_type", "✏️ Выберите направление:"), kb(rows), photo_ref_for("photo_idea"))
        return

    if step == "about":
        rows: list[list[InlineKeyboardButton]] = []
        rows.append([InlineKeyboardButton(text=get_cfg("btn_about_equipment", "🏭 Оборудование"), callback_data="about:eq")])
        rows.append([InlineKeyboardButton(text=get_cfg("btn_about_projects", "🖼 Наши проекты"), callback_data="about:projects")])
        rows.append([InlineKeyboardButton(text=get_cfg("btn_about_contacts", "📞 Контакты"), callback_data="about:contacts")])
        rows.append([InlineKeyboardButton(text=get_cfg("btn_about_map", "📍 На карте"), callback_data="about:map")])
        rows.append(nav_row(False))
        await send_step_cb(cb, get_cfg("about_text", "🏢 Chel3D — 3D-печать, моделирование и сканирование.\nВыберите раздел:"), kb(rows), photo_ref_for("photo_about"))
        return

    if cb.message:
        await show_main(cb.message, state)
    await cb.answer()


async def go_back(cb: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    history: list[str] = data.get("history", [])
    if not history:
        if cb.message:
            await show_main(cb.message, state)
        await cb.answer()
        return
    prev = history.pop()
    await state.update_data(history=history)
    await render_step(cb, state, prev, from_back=True)


async def send_order_to_orders_chat(bot: Bot, order_id: int, summary: str) -> None:
    raw_chat = get_orders_chat_id()
    if not raw_chat:
        return

    contact_block = ""
    order = database.get_order(order_id) if order_id else None
    if order:
        full_name = order.get("full_name") or "Без имени"
        username = order.get("username")
        username_line = f"@{username}" if username else "нет username"
        user_id = int(order.get("user_id") or 0)
        contact_block = (
            f"👤 Клиент: {full_name}\n"
            f"🔖 Username: {username_line}\n"
            f"🆔 Telegram ID: {user_id}\n"
            f"🔗 tg://user?id={user_id}\n\n"
        )

    chat_id = normalize_chat_id(raw_chat)
    try:
        await bot.send_message(chat_id=chat_id, text=f"🆕 Заявка №{order_id}\n\n{contact_block}{summary}")
    except Exception:
        logger.exception("Не удалось отправить заявку в чат заказов")


async def forward_order_files_to_orders_chat(bot: Bot, order_id: int) -> None:
    raw_chat = get_orders_chat_id()
    if not raw_chat or not order_id:
        return

    chat_id = normalize_chat_id(raw_chat)
<<<<<<< codex/fix-chat-loading-in-crm-requests-coc4rf
    files = database.list_order_files(order_id)
    for item in files:
=======
    try:
        files = database.list_order_files(order_id)
    except Exception:
        logger.exception("Не удалось получить файлы заявки из БД")
        return

    for item in files or []:
>>>>>>> main
        tg_file_id = item.get("telegram_file_id")
        if not tg_file_id:
            continue
        file_type = str(item.get("file_type") or item.get("mime_type") or "").lower()
        try:
            if file_type == "photo" or file_type.startswith("image/"):
                await bot.send_photo(chat_id=chat_id, photo=tg_file_id, caption=f"📎 Фото к заявке №{order_id}")
            else:
                await bot.send_document(chat_id=chat_id, document=tg_file_id, caption=f"📎 Файл к заявке №{order_id}")
        except Exception:
            logger.exception("Не удалось переслать вложение заявки в чат заказов")


<<<<<<< codex/fix-chat-loading-in-crm-requests-coc4rf
=======
async def forward_file_to_orders_chat(message: Message, order_id: int) -> None:
    raw_chat = get_orders_chat_id()
    if not raw_chat:
        return
    chat_id = normalize_chat_id(raw_chat)

    try:
        if message.photo:
            await message.bot.send_photo(
                chat_id=chat_id,
                photo=message.photo[-1].file_id,
                caption=f"📎 Фото к заявке №{order_id}",
            )
        elif message.document:
            await message.bot.send_document(
                chat_id=chat_id,
                document=message.document.file_id,
                caption=f"📎 Файл к заявке №{order_id}",
            )
    except Exception:
        logger.exception("Не удалось переслать файл в чат заказов")


>>>>>>> main

async def submit_order(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    order_id = int(data.get("order_id", 0) or 0)
    payload: dict[str, Any] = data.get("payload", {})
    summary = payload_summary(payload)
    should_forward_files = False

    if order_id:
        order = database.get_order(order_id) or {}
        should_forward_files = (order.get("status") or "draft") in {"draft", ""}

    if order_id:
        database.finalize_order(order_id, summary)
    await send_order_to_orders_chat(message.bot, order_id, summary)
<<<<<<< codex/fix-chat-loading-in-crm-requests-coc4rf
    if should_forward_files:
        await forward_order_files_to_orders_chat(message.bot, order_id)
=======
    await forward_order_files_to_orders_chat(message.bot, order_id)
>>>>>>> main

    ok_text = get_cfg("text_submit_ok", "✅ Заявка отправлена! Менеджер скоро напишет вам в этот чат.")
    await send_step(message, ok_text, kb([nav_row(include_back=False)]))
    await state.clear()


async def on_start(message: Message, state: FSMContext) -> None:
    await show_main(message, state)


async def on_menu(cb: CallbackQuery, state: FSMContext) -> None:
    branch = (cb.data or "").split(":", 1)[1] if cb.data else ""
    if branch == "about":
        await render_step(cb, state, "about")
        return
    if branch not in {"print", "scan", "idea"}:
        if cb.message:
            await show_main(cb.message, state)
        await cb.answer()
        return
    await start_order(cb, state, branch)
    await render_step(cb, state, {"print": "print_tech", "scan": "scan_type", "idea": "idea_type"}[branch])


async def on_nav(cb: CallbackQuery, state: FSMContext) -> None:
    action = (cb.data or "").split(":", 1)[1]
    if action == "menu":
        if cb.message:
            await show_main(cb.message, state)
        await cb.answer()
        return
    if action == "back":
        await go_back(cb, state)
        return
    await cb.answer()


async def on_about(cb: CallbackQuery, state: FSMContext) -> None:
    key = (cb.data or "").split(":", 1)[1]
    mapping = {
        "eq": ("about_equipment_text", "photo_about_equipment"),
        "projects": ("about_projects_text", "photo_about_projects"),
        "contacts": ("about_contacts_text", "photo_about_contacts"),
        "map": ("about_map_text", "photo_about_map"),
    }
    cfg_key, photo_key = mapping.get(key, ("about_text", "photo_about"))
    await send_step_cb(cb, get_cfg(cfg_key, "ℹ️ О нас"), kb([nav_row()]), photo_ref_for(photo_key))
    await persist(state)


async def on_set(cb: CallbackQuery, state: FSMContext) -> None:
    parts = (cb.data or "").split(":", 2)
    if len(parts) < 3:
        await cb.answer()
        return
    _, field, value = parts

    st = await state.get_data()
    payload: dict[str, Any] = st.get("payload", {})
    payload[field] = value
    await state.update_data(payload=payload)
    await persist(state)

    if field == "technology":
        await render_step(cb, state, "print_material")
        return

    if field == "material":
        if "🤔" in value:
            await render_step(cb, state, "print_material_custom")
            return
        await render_step(cb, state, "attach_file")
        return

    if field in {"scan_type", "idea_type"}:
        await render_step(cb, state, "review")
        return

    if field == "file":
        await render_step(cb, state, "review")
        return

    await cb.answer()


async def on_text(message: Message, state: FSMContext) -> None:
    st = await state.get_data()
    waiting = st.get("waiting_text")
    if not waiting:
        return

    payload: dict[str, Any] = st.get("payload", {})

    if waiting == "material_custom":
        user_text = (message.text or "").strip()
        payload["material_custom"] = user_text
        await state.update_data(payload=payload, waiting_text=None)
        await persist(state)
        if st.get("order_id") and user_text:
            try:
                database.add_order_message(int(st["order_id"]), "in", user_text)
            except Exception:
                logger.exception("Не удалось сохранить входящее сообщение (material_custom)")
        await send_step(message, "Принято ✅", kb([nav_row()]))

        fake_cb = CallbackQuery(id="0", from_user=message.from_user, chat_instance="0", message=message, data="")
        await render_step(fake_cb, state, "attach_file")
        return

    if waiting == "description":
        user_text = (message.text or "").strip()
        payload["description"] = user_text
        await state.update_data(payload=payload, waiting_text=None)
        await persist(state)
        if st.get("order_id") and user_text:
            try:
                database.add_order_message(int(st["order_id"]), "in", user_text)
            except Exception:
                logger.exception("Не удалось сохранить входящее сообщение (description)")
<<<<<<< codex/fix-chat-loading-in-crm-requests-coc4rf
=======

        # ВАЖНО: не автосабмитим. Возвращаемся в review.
>>>>>>> main
        await send_step(message, "Описание добавлено ✅", review_keyboard())
        return


async def on_file(message: Message, state: FSMContext) -> None:
    st = await state.get_data()
    order_id = int(st.get("order_id", 0) or 0)
    if not order_id:
        return

    tg_file_id = None
    file_unique_id = None
    file_name = None
    file_type = None

    if message.document:
        tg_file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        file_name = message.document.file_name
        file_type = "document"
    elif message.photo:
        tg_file_id = message.photo[-1].file_id
        file_unique_id = message.photo[-1].file_unique_id
        file_name = f"photo_{tg_file_id}.jpg"
        file_type = "photo"
    else:
        return

    try:
        database.add_order_file(order_id, tg_file_id, file_unique_id, file_name, file_type)
    except Exception:
        logger.exception("Не удалось записать файл в БД")

    try:
        f = await message.bot.get_file(tg_file_id)
        dst = UPLOADS_DIR / f"{order_id}_{Path(file_name or tg_file_id).name}"
        await message.bot.download_file(f.file_path, destination=dst)
    except Exception:
        logger.exception("Не удалось скачать файл локально")

    payload: dict[str, Any] = st.get("payload", {})
    payload["file"] = file_name or "файл"
    await state.update_data(payload=payload)
    await persist(state)

    await forward_file_to_orders_chat(message, order_id)

    fake_cb = CallbackQuery(id="0", from_user=message.from_user, chat_instance="0", message=message, data="")
    await render_step(fake_cb, state, "review")


async def on_review(cb: CallbackQuery, state: FSMContext) -> None:
    action = (cb.data or "").split(":", 1)[1] if cb.data else ""
    if action == "add_description":
        await render_step(cb, state, "description")
        return
    if action == "send":
        if cb.message:
            await submit_order(cb.message, state)
        await cb.answer()
        return
    await cb.answer()


async def handle_internal_send_message(request: web.Request) -> web.Response:
    key = request.headers.get("X-Internal-Key", "")
    if not key or key != settings.internal_api_key:
        return web.json_response({"detail": "Unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"detail": "Bad JSON"}, status=400)

    user_id = int(data.get("user_id", 0) or 0)
    text = str(data.get("text", "") or "").strip()
    order_id = int(data.get("order_id", 0) or 0)

    if not user_id or not text:
        return web.json_response({"detail": "user_id и text обязательны"}, status=400)

    bot: Bot = request.app["bot"]
    try:
        await bot.send_message(chat_id=user_id, text=text)
    except Exception:
        logger.exception("Не удалось отправить сообщение пользователю")
        return web.json_response({"detail": "Telegram send failed"}, status=400)

    if order_id:
        try:
            database.add_order_message(order_id, "out", text)
        except Exception:
            logger.exception("Не удалось сохранить сообщение в БД")

    return web.json_response({"ok": True})


async def start_internal_api(bot: Bot) -> web.AppRunner:
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/internal/sendMessage", handle_internal_send_message)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8081)
    await site.start()
    return runner


async def main() -> None:
    database.init_db_if_needed()

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(on_start, CommandStart())
    dp.callback_query.register(on_menu, F.data.startswith("menu:"))
    dp.callback_query.register(on_nav, F.data.startswith("nav:"))
    dp.callback_query.register(on_about, F.data.startswith("about:"))
    dp.callback_query.register(on_set, F.data.startswith("set:"))
    dp.callback_query.register(on_review, F.data.startswith("review:"))

    dp.message.register(on_text, F.text)
    dp.message.register(
        on_file,
        F.content_type.in_({ContentType.DOCUMENT, ContentType.PHOTO}),
    )

    runner = await start_internal_api(bot)

    try:
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
