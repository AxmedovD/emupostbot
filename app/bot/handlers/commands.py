from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.enums import ChatType
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove
from aiogram.utils.i18n import gettext as _, lazy_gettext as __
from aiogram.utils.markdown import hbold

from app.bot.keyboards.reply import (
    get_language_keyboard,
    share_phone_keyboard,
    get_main_keyboard
)
from app.bot.states.registration import RegistrationStates
from app.db.pool import Database
from app.core.logger import bot_logger
from app.core.utils import clean_phone_number

router = Router(name="commands")

# Barcha handler'lar faqat private chat uchun
router.message.filter(F.chat.type == ChatType.PRIVATE)

# Konstanta
SUPPORTED_LANGUAGES = {
    "🇺🇿 O'zbek": "uz",
    "🇷🇺 Русский": "ru",
    "🇬🇧 English": "en"
}

SUCCESS_MESSAGES = {
    "uz": "✅ Til muvaffaqiyatli o'zgartirildi!\n\nQayta boshlash: /start",
    "ru": "✅ Язык успешно изменен!\n\nПерезапустить: /start",
    "en": "✅ Language changed successfully!\n\nRestart: /start"
}


async def get_or_create_user(
        message: Message,
        db: Database,
        state: FSMContext
) -> tuple[dict | None, bool]:
    """
    Foydalanuvchini olish yoki yaratish

    Returns:
        tuple: (user_data, is_new_user)
    """
    telegram_id = message.from_user.id

    # Foydalanuvchini topish
    user = await db.read(
        table="users",
        conditions={"telegram_id": telegram_id},
        limit=1,
        result_type="row"
    )

    if user is not None:
        return user, False

    # Yangi foydalanuvchi yaratish
    try:
        username = message.from_user.username or str(telegram_id)
        user_id = await db.create(
            table="users",
            data={
                "name": message.from_user.full_name or str(telegram_id),
                "telegram_id": telegram_id,
                "phone": str(telegram_id),
                "username": username.lower(),
                "password": str(telegram_id),
                "lang": "uz"
            }
        )

        await state.set_state(RegistrationStates.language)
        await state.update_data(user_id=user_id, is_new=True)

        return {"id": user_id, "lang": "uz"}, True

    except Exception as e:
        bot_logger.error(f"User yaratishda xatolik: {e}")
        return None, False


@router.message(CommandStart())
async def cmd_start(message: Message, db: Database, state: FSMContext):
    """Start buyrug'i"""
    user, is_new = await get_or_create_user(message, db, state)

    if user is None:
        return await message.answer(
            hbold(_("Xatolik yuz berdi. Qayta urinib ko'ring: /start"))
        )

    # Yangi foydalanuvchi uchun
    if is_new:
        return await message.answer(
            text=hbold(_("Botga xush kelibsiz!")),
            reply_markup=get_language_keyboard()
        )

    # Ro'yxatdan o'tish jarayonida
    current_state = await state.get_state()
    if current_state is not None:
        return await message.answer(
            text=hbold(_("Botga xush kelibsiz!")),
            reply_markup=get_language_keyboard()
        )

    # Asosiy menyu
    return await message.answer(
        text=hbold(_("Xush kelibsiz!")),
        reply_markup=get_main_keyboard()
    )


@router.message(
    RegistrationStates.language,
    F.text.in_(list(SUPPORTED_LANGUAGES.keys()))
)
async def select_language(message: Message, db: Database, state: FSMContext):
    """Tilni tanlash"""
    selected_language = SUPPORTED_LANGUAGES.get(message.text)

    if not selected_language:
        return await message.answer(
            hbold(_("Noto'g'ri til tanlandi!"))
        )

    try:
        await db.update(
            table='users',
            data={'lang': selected_language},
            conditions={'telegram_id': message.from_user.id}
        )

        data = await state.get_data()
        is_new = data.get('is_new', False)

        if is_new:
            await state.set_state(RegistrationStates.name)
            return await message.answer(
                text=hbold(_("Iltimos, ism va familiyangizni yuboring.", locale=selected_language)),
                reply_markup=ReplyKeyboardRemove()
            )

        await state.clear()
        return await message.answer(
            text=hbold(SUCCESS_MESSAGES[selected_language]),
            reply_markup=get_main_keyboard()
        )

    except Exception as e:
        bot_logger.error(f"Tilni saqlashda xatolik: {e}")
        return await message.answer(
            hbold(_("Xatolik yuz berdi. Qayta urinib ko'ring."))
        )


@router.message(RegistrationStates.name, F.text)
async def get_name(message: Message, state: FSMContext):
    """Ism va familiya olish"""
    name = message.text.strip()

    # Validatsiya
    if len(name) < 3:
        return await message.answer(
            hbold(_("Ism juda qisqa. Kamida 3 ta harf kiriting."))
        )

    if len(name) > 100:
        return await message.answer(
            hbold(_("Ism juda uzun. Maksimal 100 ta harf."))
        )

    await state.update_data(name=name)
    await state.set_state(RegistrationStates.phone)

    return await message.answer(
        text=hbold(_("Iltimos, telefon raqamingizni yuboring.")),
        reply_markup=share_phone_keyboard()
    )


@router.message(RegistrationStates.phone, F.contact)
async def get_phone(message: Message, db: Database, state: FSMContext):
    """Telefon raqam olish"""
    # Faqat o'z telefon raqamini qabul qilish
    if message.contact.user_id != message.from_user.id:
        return await message.answer(
            text=hbold(_("Iltimos, o'z telefon raqamingizni yuboring!")),
            reply_markup=share_phone_keyboard()
        )

    try:
        data = await state.get_data()
        phone = clean_phone_number(message.contact.phone_number)

        await db.update(
            table='users',
            data={
                "name": data.get('name'),
                "phone": phone,
            },
            conditions={"id": data.get('user_id')}
        )

        await state.clear()

        return await message.answer(
            text=hbold(_("Ro'yxatdan muvaffaqiyatli o'tdingiz!")),
            reply_markup=get_main_keyboard()
        )

    except Exception as e:
        bot_logger.error(f"Telefon raqamni saqlashda xatolik: {e}")
        return await message.answer(
            hbold(_("Xatolik yuz berdi. Qayta urinib ko'ring."))
        )


@router.message(RegistrationStates.phone)
async def invalid_phone_format(message: Message):
    """Noto'g'ri format uchun xabar"""
    return await message.answer(
        text=hbold(_("Iltimos, faqat tugmani bosing!")),
        reply_markup=share_phone_keyboard()
    )
