from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'train_arrivals',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

station_delay_counts = Counter()
total_delay_minutes = defaultdict(float)
msg_count = 0

print("Rozpoczęto nasłuchiwanie. Zbieram statystyki opóźnień...\n")

for message in consumer:
    train_data = message.value
    
    # Bezpieczne pobieranie wartości z Kafki
    station = train_data.get('station', 'Nieznana')
    delay = train_data.get('delay_minutes', 0)
    
    msg_count += 1
    
    # Aktualizujemy stan tylko dla spóźnionych pociągów
    if delay > 0:
        station_delay_counts[station] += 1
        total_delay_minutes[station] += delay

    # Wypisywanie tabeli co 10 wiadomości
    if msg_count % 10 == 0:
        print(f"\n{'='*70}")
        print(f"{'Stacja':<30} {'Liczba opóźnień':>17} {'Średnie opóźnienie':>18}")
        print(f"{'-'*70}")
      
        if not station_delay_counts:
            print("Brak opóźnień do tej pory.")
            
        for s in sorted(station_delay_counts):
            n = station_delay_counts[s]
            tot = total_delay_minutes[s]
            
            avg_delay = (tot / n) if n > 0 else 0.0
            
            print(f"{s:<30} {n:>17} {avg_delay:>18.2f} min")
            
        print(f"{'='*70}")
        print(f"Łącznie odebranych wiadomości od startu: {msg_count}\n")
