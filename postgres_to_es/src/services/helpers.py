from typing import Iterable, Optional

from models import Person


# Helper functions
def dispatcher(row: Iterable, name: str) -> Optional[dict]:
    _dict = {
        'genres': (lambda row: [genre.lower() for genre in row]),
        'persons_names': (lambda row: [item[1].lower() for item in row]),
        'persons': (lambda row: [
            Person(id=item[0], full_name=item[1].lower()) for item in row]),
        }
    if row:
        return _dict[name](row)
    else:
        return None
