def clean_phone_number(phone: str) -> str:
    if not phone:
        return ""

    # Faqat raqamlar va + belgisini qoldirish
    cleaned = ''.join(c for c in phone if c.isdigit() or c == '+')

    # Agar + belgisi yo'q bo'lsa, qo'shish
    if not cleaned.startswith('+'):
        cleaned = '+' + cleaned

    return cleaned
