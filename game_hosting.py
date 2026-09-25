HOST_LABEL = "🎙Ведуший"


def order_with_host_first(participants, host_user_id, late_user_ids=()):
    """Put punctual players before late ones and the host first in their group."""
    late_user_ids = set(late_user_ids)
    return sorted(
        participants,
        key=lambda participant: (
            participant[0] in late_user_ids,
            participant[0] != host_user_id if host_user_id is not None else False,
        ),
    )


def participant_number(index, user_id, host_user_id):
    return 0 if user_id == host_user_id else index


def add_host_label(display_name, user_id, host_user_id):
    if user_id == host_user_id:
        return f"{display_name} - {HOST_LABEL}"
    return display_name
