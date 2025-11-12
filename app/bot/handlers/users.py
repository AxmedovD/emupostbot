from datetime import datetime, timedelta

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
    get_main_keyboard,
    stop_keyboard
)
from app.bot.states.registration import RegistrationStates
from app.db.pool import Database
from app.core.logger import bot_logger
from app.core.utils import clean_phone_number, check_user_contract

router: Router = Router(name="users")

# Barcha handler'lar faqat private chat uchun
router.message.filter(F.chat.type == ChatType.PRIVATE)

# Konstanta
SUPPORTED_LANGUAGES: dict = {
    "🇺🇿 O'zbek": "uz",
    "🇷🇺 Русский": "ru",
    "🇬🇧 English": "en"
}

SUCCESS_MESSAGES: dict = {
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
    Foydalanuvchini olish yoki qo'shish

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

    # Yangi foydalanuvchi qo'shish
    try:
        user_id = await db.create(
            table="users",
            data={
                "name": message.from_user.full_name or str(telegram_id),
                "telegram_id": telegram_id,
                "phone": str(telegram_id),
                "username": str(telegram_id),
                "password": str(telegram_id),
                "lang": "uz",
                "role_code": 3,
            }
        )

        await state.set_state(RegistrationStates.language)
        await state.update_data(user_id=user_id, is_new=True)

        return {"id": user_id, "lang": "uz"}, True

    except Exception as e:
        bot_logger.error(f"User qo'shishda xatolik: {e}")
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
    data = await state.get_data()
    if data.get('step'):
        return await message.answer(
            text=hbold(_("Botga xush kelibsiz!")),
            reply_markup=get_language_keyboard()
        )
    await state.clear()

    is_contract = await check_user_contract(message.from_user.id, db)

    # Asosiy menyu
    return await message.answer(
        text=hbold(_("Xush kelibsiz!")),
        reply_markup=get_main_keyboard(is_contract=is_contract)
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
    if message.contact.user_id != message.from_user.id:
        return await message.answer(
            text=hbold(_("Iltimos, o'z telefon raqamingizni yuboring!")),
            reply_markup=share_phone_keyboard()
        )

    try:
        data: dict = await state.get_data()
        phone: str = clean_phone_number(message.contact.phone_number)
        name: str = data.get('name')
        new_user_id: int = data.get('user_id')

        await state.clear()

        # Telefon raqam USERS jadvalida avval ro'yxatdan o'tganmi?
        existing_user: dict = await db.read(
            table='users',
            conditions={'phone': phone},
            limit=1,
            result_type='row'
        )

        # Agar telefon YANGI bo'lsa → HAMMA ma'lumotlarni yangilash
        if existing_user is None:
            await db.update(
                table='users',
                data={
                    "name": name,
                    "phone": phone,
                    "role_code": 2
                },
                conditions={"id": new_user_id}
            )

            await db.update(
                table='p_sender',
                conditions={'phone': phone},
                data={"user_id": new_user_id}
            )

            await db.update(
                table='p_receiver',
                conditions={'phone': phone},
                data={"user_id": new_user_id}
            )

        # Agar telefon ESKI bo'lsa → FAQAT 1 haftalikni yangilash
        else:
            await db.delete(table='users', conditions={"id": new_user_id})
            return await message.answer(
                text=hbold(_("Kechirasiz, ushbu nomer avval ro'yxatdan o'tagn!")),
                reply_markup=ReplyKeyboardRemove(),
            )
            # one_week_ago = datetime.utcnow() - timedelta(weeks=1)
            #
            # await db.update(
            #     table='p_sender',
            #     conditions={
            #         'phone': phone,
            #         'created_at': ('>=', one_week_ago)
            #     },
            #     data={"user_id": new_user_id}
            # )
            #
            # await db.update(
            #     table='p_receiver',
            #     conditions={
            #         'phone': phone,
            #         'created_at': ('>=', one_week_ago)
            #     },
            #     data={"user_id": new_user_id}
            # )

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


@router.message(F.text == __("📄 Shartnoma"))
async def _contract(message: Message, state: FSMContext):
    await state.set_state(RegistrationStates.login)
    return await message.answer(
        text=hbold(_("Iltimos, kirish uchun loginni yuboring!")),
        reply_markup=stop_keyboard()
    )


@router.message(F.text == __("❎ Bekor qilish"))
async def _stop(message: Message, state: FSMContext):
    await state.clear()
    return await message.answer(
        text=hbold(_("Xush kelibsiz!")),
        reply_markup=get_main_keyboard()
    )


@router.message(RegistrationStates.login, F.text)
async def get_login(message: Message, state: FSMContext):
    login: str = message.text.strip()
    await state.update_data(login=login)
    await state.set_state(RegistrationStates.password)
    return await message.answer(
        text=hbold(_("Iltimos, parolni yuboring!")),
        reply_markup=stop_keyboard()
    )


@router.message(RegistrationStates.password, F.text)
async def get_password(message: Message, state: FSMContext, db: Database):
    data: dict = await state.get_data()
    login: str = data.get('login')
    password: str = message.text.strip()
    await state.clear()

    client: dict = await db.read(
        table='clients',
        conditions={'api_login': login, "api_pass": password},
        limit=1,
        result_type='row'
    )
    if client is None:
        return await message.answer(
            text=hbold(_("Kechirasiz, Login yoki Parol xato!"))
        )
    if client.get('user_id') is not None:
        return await message.answer(
            text=hbold(_("Kechirasiz, ushbu mijoz avval ro'yxatdan o'tgan!")),
            reply_markup=get_main_keyboard()
        )
    user_id: int = await db.read(
        table='users',
        conditions={'telegram_id': message.from_user.id},
        fields=['id'],
        limit=1,
        result_type='val'
    )
    await db.update(
        table='clients',
        data={"user_id": user_id},
        conditions={'id': client.get('id')},
    )
    return await message.answer(text=hbold(_("Ro'yxatdan o'tildi!")), reply_markup=get_main_keyboard(is_contract=True))
