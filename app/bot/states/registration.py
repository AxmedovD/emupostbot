from aiogram.fsm.state import State, StatesGroup


class RegistrationStates(StatesGroup):
    """Ro'yxatdan o'tish holatlari"""
    language = State()  # Til tanlash
    name = State()  # Ism kiritish
    phone = State()  # Telefon raqam
    login = State()  # Shartnoma Logini
    password = State()  # Shartnoma Paroli
