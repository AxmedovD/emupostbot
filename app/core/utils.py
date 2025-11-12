from re import findall, match
from typing import Dict, Set, Any, Tuple
from collections import defaultdict

from app.db.pool import Database


async def check_user_contract(telegram_id: int, db: Database) -> bool:
    """Foydalanuvchining shartnomasi bor-yo'qligini tekshirish"""
    try:
        return await db.pool.fetchval(
            "SELECT EXISTS(SELECT 1 FROM clients c INNER JOIN users u ON c.user_id = u.id WHERE u.telegram_id = $1 AND c.user_id IS NOT NULL)",
            telegram_id
        ) or False
    except Exception:
        return False


def clean_phone_number(phone: str) -> str:
    if not phone:
        return ""

    return ''.join(c for c in phone if c.isdigit())


def parse_template(message_template: str, include_phone: bool = True) -> Dict[str, Set[str]]:
    """
    Template dan jadval va ustun nomlarini parse qilish

    Args:
        message_template: "Hurmatli {user.first_name}, buyurtma {order.number}"
        include_phone: Agar True bo'lsa, user.phone ni avtomatik qo'shadi

    Returns:
        {'user': {'first_name', 'phone'}, 'order': {'number'}}
    """
    pattern: str = r'\{(\w+)\.(\w+)\}'
    matches: list = findall(pattern, message_template)

    fields: dict = defaultdict(set)
    for table, column in matches:
        fields[table].add(column)

    if include_phone and 'user' in fields:
        fields['user'].add('phone')

    return dict(fields)


def render_template(message_template: str, data: Dict[str, Dict[str, Any]]) -> str:
    """
    Template ni ma'lumotlar bilan to'ldirish

    Args:
        message_template: "Hurmatli {user.first_name}, buyurtma {order.number}"
        data: {
            'user': {'first_name': 'Ali', 'phone': '+998901234567'},
            'order': {'number': 'ORD-123'}
        }

    Returns:
        "Hurmatli Ali, buyurtma ORD-123"
    """
    message: str = message_template
    pattern: str = r'\{(\w+)\.(\w+)\}'
    matches: list = findall(pattern, message_template)

    for table, column in matches:
        placeholder: str = f"{{{table}.{column}}}"
        value: str = data.get(table, {}).get(column, '')

        if value is None:
            value = ''

        message = message.replace(placeholder, str(value))

    return message


def validate_template(message_template: str) -> Tuple[bool, str]:
    if not message_template or not message_template.strip():
        return False, "Template bo'sh bo'lishi mumkin emas"

    pattern: str = r'\{(\w+)\.(\w+)\}'
    matches: list = findall(pattern, message_template)

    if not matches:
        return False, "Template da hech qanday placeholder topilmadi. Format: {table.column}"

    invalid_pattern: str = r'\{[^}]*\}'
    all_placeholders: list = findall(pattern=invalid_pattern, string=message_template)

    for placeholder in all_placeholders:
        if not match(pattern=r'^\{\w+\.\w+\}$', string=placeholder):
            return False, f"Noto'g'ri placeholder: {placeholder}. To'g'ri format: {{table.column}}"

    return True, ""
