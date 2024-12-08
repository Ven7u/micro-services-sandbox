import random

import httpx

movies = [
    "The Shawshank Redemption",
    "The Dark Knight",
    "Inception",
    "Forrest Gump",
    "The Godfather",
    "Pulp Fiction",
    "The Lord of the Rings: The Return of the King",
    "Parasite",
    "Avengers: Endgame",
    "Interstellar",
    "Gladiator",
]


reviews = [
    ("An absolute masterpiece with breathtaking visuals.", 9),
    ("The storyline felt rushed and lacked depth.", 5),
    ("Amazing performances by the cast and a great script.", 8),
    ("The plot twists were predictable and uninspired.", 4),
    ("A heartwarming story that left me in tears.", 9),
    ("Overhyped and not as good as I expected.", 6),
    ("The cinematography alone is worth watching this film.", 10),
    ("Poor character development and a weak ending.", 3),
    ("A delightful movie with plenty of laughs.", 8),
    ("The pacing was off, making it a slog to get through.", 4),
    ("Brilliantly directed with a powerful message.", 9),
    ("A generic action movie with nothing new to offer.", 5),
    ("Highly creative and refreshingly original.", 10),
    ("The dialogue was cringeworthy and unrealistic.", 3),
    ("A touching story with relatable characters.", 8),
    ("Confusing plot and overly complicated narrative.", 4),
    ("Visually stunning but lacking substance.", 7),
    ("Perfectly captures the essence of its genre.", 9),
    ("A disappointing sequel that fails to deliver.", 5),
    ("An inspiring tale with a memorable soundtrack.", 9),
]


# content = {
#     "user_id": "1",
#     "title": "Gladiator",
#     "review": "Baaad!",
#     "score": 4.0,
# }

rnd_movie = random.choice(movies)
rnd_review = random.choice(reviews)

content = {
    "user_id": "1",
    "title": rnd_movie,
    "review": rnd_review[0],
    "score": rnd_review[1],
}


response = httpx.post("http://localhost:8080/ingest", json=content)
print(response.status_code, str(response.content))
