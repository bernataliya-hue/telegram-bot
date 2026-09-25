"""Manual members have no messaging account and use a reserved ID range."""
PLATFORM_MANUAL = 'manual'
MANUAL_ID_START = -(2 ** 62)


def detect_platform_by_user_id(user_id):
    user_id = int(user_id)
    if user_id <= MANUAL_ID_START:
        return PLATFORM_MANUAL
    return 'vk' if user_id < 0 else 'telegram'
