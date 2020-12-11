from dataclasses import dataclass
from typing import List, Optional, Iterable


# Helper structures
@dataclass
class Person:
    id: str
    full_name: str

@dataclass
class Movie:
    id: str
    title: str
    description: str
    rating: float
    genres: List[str]
    actors_names: List[str]
    writers_names: List[str]
    directors_names: List[str]
    actors: List[Person]
    writers: List[Person]
    directors: List[Person]


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
