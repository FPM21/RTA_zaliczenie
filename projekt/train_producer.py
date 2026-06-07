from kafka import KafkaProducer
import json, random, time
from datetime import datetime, timedelta

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Połączenia PKP: id pociągu, linia, stacje pośrednie
CONNECTIONS = [
    {'train_id': 'IC103', 'line': 'Warszawa–Kraków', 'stations': ['Warszawa Centralna', 'Radom', 'Kielce', 'Kraków Główny']},
    {'train_id': 'TLK15', 'line': 'Warszawa–Gdańsk', 'stations': ['Warszawa Centralna', 'Bydgoszcz Główna', 'Tczew', 'Gdańsk Główny']},
    {'train_id': 'EIC21', 'line': 'Warszawa–Wrocław', 'stations': ['Warszawa Centralna', 'Łódź Fabryczna', 'Piotrków Trybunalski', 'Wrocław Główny']},
    {'train_id': 'IC57', 'line': 'Warszawa–Poznań', 'stations': ['Warszawa Centralna', 'Kutno', 'Konin', 'Poznań Główny']},
    {'train_id': 'TLK88', 'line': 'Kraków–Gdańsk', 'stations': ['Kraków Główny', 'Częstochowa', 'Bydgoszcz Główna', 'Gdańsk Główny']},
    {'train_id': 'EIC34', 'line': 'Wrocław–Poznań', 'stations': ['Wrocław Główny', 'Leszno', 'Poznań Główny']},
    {'train_id': 'IC72', 'line': 'Warszawa–Łódź', 'stations': ['Warszawa Centralna', 'Warszawa Zachodnia', 'Skierniewice', 'Łódź Fabryczna']},
    {'train_id': 'TLK49', 'line': 'Kraków–Poznań', 'stations': ['Kraków Główny', 'Katowice', 'Gliwice', 'Poznań Główny']},
    {'train_id': 'EIC90', 'line': 'Gdańsk–Wrocław', 'stations': ['Gdańsk Główny', 'Bydgoszcz Główna', 'Poznań Główny', 'Leszno', 'Wrocław Główny']},
    {'train_id': 'IC61', 'line': 'Warszawa–Katowice', 'stations': ['Warszawa Centralna', 'Radom', 'Kielce', 'Sosnowiec Główny', 'Katowice']},
]

def generate_delay():
    roll = random.random()
    if roll < 0.55: return 0
    elif roll < 0.80: return random.randint(1, 4)
    elif roll < 0.92: return random.randint(5, 14)
    else: return random.randint(15, 60)

def generate_train_event():
    conn = random.choice(CONNECTIONS)
    station = random.choice(conn['stations'])
    base_hour = random.randint(5, 22)
    base_minute = random.choice([0, 10, 15, 20, 30, 40, 45, 50])
    scheduled = datetime.now().replace(hour=base_hour, minute=base_minute, second=0, microsecond=0)
    delay = generate_delay()
    actual = scheduled + timedelta(minutes=delay)
    return {
        'train_id': conn['train_id'],
        'line': conn['line'],
        'station': station,
        'scheduled_arrival': scheduled.strftime('%H:%M'),
        'actual_arrival': actual.strftime('%H:%M'),
        'delay_minutes': delay,
        'timestamp': datetime.now().isoformat(),
    }

print("  PKP Monitor — producent opóźnień pociągów")
print("------------------------------------------"

for i in range(750):
    event = generate_train_event()
    
    # numer porzadkowy do alertow
    event['event_index'] = i + 1 
    
    producer.send('train_arrivals', value=event)

    print(
        f"[{event['event_index']:>3}] {event['train_id']:<6} | {event['station']:<26} | "
        f"plan: {event['scheduled_arrival']} | "
        f"rzecz: {event['actual_arrival']} | "
        f"+{event['delay_minutes']} min"
    )
    time.sleep(1)

producer.flush()
producer.close()
print("\nProducent zakończył pracę.")
