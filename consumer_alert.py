from kafka import KafkaConsumer
import json
import time

# Konfiguracja
DELAY_THRESHOLD = 1 
TOPIC = 'train_arrivals'

# Kolory do terminala
RED = '\033[91m'
BLUE = '\033[94m' 
RESET = '\033[0m'
BOLD = '\033[1m'

# Dynamiczne group_id gwarantuje, że przy każdym uruchomieniu konsument ignoruje stare dane i czeka na nowe
unique_group_id = f'monitor-group-{int(time.time())}'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='broker:9092',
    group_id=unique_group_id, 
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"{BOLD}SYSTEM MONITORINGU PKP W CZASIE RZECZYWISTYM {RESET}")
print(f" Nasłuchuję na pociągi opóźnione powyżej {DELAY_THRESHOLD} min")

current_batch_delays = []

try:
    for message in consumer:
        event = message.value
        
        # pobieranie numeru wygenerowanego przez producera
        prod_nr = event.get('event_index', 0)
        
        if 'delay_minutes' in event:
            delay = event['delay_minutes']
            
            if delay >= DELAY_THRESHOLD:
                current_batch_delays.append(delay)

                color = RED if delay >= 20 else BLUE
                
                print(f"{color}{BOLD}PRZEJAZD [{prod_nr}], {event['train_id']}, {event['line']}, "
                      f"{event['scheduled_arrival']} -> {event['actual_arrival']} ({delay}MIN) OPÓŹNIENIE!{RESET}")
                print("-" * 80)

        # raport co 10 pociągów
        if prod_nr > 0 and prod_nr % 10 == 0:
            start_range = prod_nr - 9
            print(f"\n{BOLD}[RAPORT OKRESOWY - POCIĄGI {start_range} DO {prod_nr}]{RESET}")
            
            if len(current_batch_delays) > 0:
                #liczenie sredniej z calej 10
                avg_all = sum(current_batch_delays) / 10
                print(f" Suma opóźnień: {sum(current_batch_delays)} min")
                print(f" Ilość incydentów: {len(current_batch_delays)}")
            else:
                print(" Wszystkie pociągi w tej partii odjechały punktualnie!")
            
            print("-" * 30 + "\n")
            
            # czyszczenie listy na kolejną dziesiątkę
            current_batch_delays = []
        
except KeyboardInterrupt:
    print("\nZamykanie systemu...")
finally:
    consumer.close()