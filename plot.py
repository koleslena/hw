import pandas as pd
import matplotlib.pyplot as plt
import sched
import time
import os

os.makedirs('logs', exist_ok=True)

_LOGS = 'logs/metric_log.csv'
_PLOTS = 'logs/error_distribution.png'

_scheduler = sched.scheduler(time.time, time.sleep)

def _make_plot():
    print("Run make plot")
    try:
        if os.path.exists(_LOGS):
            df = pd.read_csv(_LOGS)
            print(f"Loaded data: {df.shape}")
            
            if len(df) > 0:
                plt.figure(figsize=(10, 6))
                plt.hist(df['absolute_error'], bins=20, alpha=0.7, color='green')
                plt.title('Распределение абсолютных ошибок модели')
                plt.xlabel('Абсолютная ошибка')
                plt.ylabel('Частота')
                plt.grid(True, alpha=0.3)
                
                plt.savefig(_PLOTS)
                plt.close()
                
                print(f"Plot was made")
        
    except Exception as e:
        print(f"Make plot error: {e}")
    finally:
        print("Run make plot finished")


def schedule_next_event():
    _scheduler.enter(10, 1, _make_plot)
    _scheduler.run()

_scheduler.enter(10, 1, _make_plot)

while True:
    schedule_next_event()
