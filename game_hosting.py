HOST_LABEL = "Ведуший"


def order_with_host_first(participants, host_user_id):
    """Put the selected host first without changing the order of other players."""
    if host_user_id is None:
        return list(participants)
    return sorted(participants, key=lambda participant: participant[0] != host_user_id)


def participant_number(index, user_id, host_user_id):
    return 0 if user_id == host_user_id else index


def add_host_label(display_name, user_id, host_user_id):
    if user_id == host_user_id:
        return f"{display_name} - {HOST_LABEL}"
    return display_name
