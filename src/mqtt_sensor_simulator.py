"""
07_mqtt_sensor_simulator.py
CANLI RASTGELE VERİ ÜRETİCİSİ
Dosyadan okumaz, anlık olarak matematiksel modellerle veri üretir.
"""

import json
import time
import random
import numpy as np
from datetime import datetime
import paho.mqtt.client as mqtt

class MQTTSensorNode:
    def __init__(self, node_id, broker='broker.hivemq.com', port=1883):
        self.node_id = node_id
        self.broker = broker
        self.port = port
        
        self.client = mqtt.Client(client_id=f"sensor_gen_{node_id}")
        self.publish_topic = f"iot/sensors/{node_id}/data"
        self.connected = False
        self.client.on_connect = self.on_connect
        
        # Simülasyon Durumu (Her sensörün kendi durumu var)
        self.cycle = 0
        self.health = 100.0
        self.degradation_rate = random.uniform(0.01, 0.05) # Her sensör farklı hızda bozulur
        
        # Temel Değerler (Her sensör biraz farklı başlar)
        self.base_temp = 520 + random.uniform(-5, 5)
        self.base_vib = 0.02 + random.uniform(0, 0.01)
        
        print(f"[Sensör {self.node_id}] Canlı simülasyon başlatıldı.")

    def connect(self):
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            time.sleep(0.5)
            return True
        except:
            return False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0: self.connected = True

    def generate_realtime_data(self):
        """
        Anlık, rastgele ama gerçekçi sensör verisi üretir.
        """
        self.cycle += 1
        
        # Rastgelelik: Bazen anlık ani bozulmalar olsun (%2 ihtimal)
        is_sudden_spike = random.random() < 0.02
        
        # Sağlığı azalt (zamanla yıpranma)
        if self.health > 0:
            drop = self.degradation_rate
            if is_sudden_spike: drop *= 10 # Ani hasar
            self.health -= drop
        
        # Sağlık durumuna göre veriyi boz (Degradasyon faktörü)
        deg_factor = (100 - self.health) / 100
        if deg_factor < 0: deg_factor = 0
        
        # Veri Üretimi (Normal Dağılım + Degradasyon Etkisi)
        # Sağlık kötüleştiğinde titreşim ve sıcaklık artar
        temp1 = self.base_temp + (deg_factor * 50) + np.random.normal(0, 2)
        temp2 = 640 + (deg_factor * 60) + np.random.normal(0, 3)
        
        # Spike varsa o anlık fırla
        if is_sudden_spike:
            temp1 += 30
            temp2 += 40
            
        pressure = 14.5 + (deg_factor * 3) + np.random.normal(0, 0.3)
        
        # Titreşim en kritik gösterge
        vibration = self.base_vib + (deg_factor * 0.2) + np.random.normal(0, 0.005)
        if is_sudden_spike: vibration += 0.1
            
        rpm = 2300 + (deg_factor * 250) + np.random.normal(0, 20)

        # Veri Paketi
        reading = {
            'node_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'cycle': self.cycle,
            'measurements': {
                'temperature_1': round(temp1, 2),
                'temperature_2': round(temp2, 2),
                'pressure': round(pressure, 2),
                'vibration': round(vibration, 4),
                'rpm': round(rpm, 1)
            },
            'health': round(self.health, 2)
        }
        return reading

    def read_and_publish(self):
        if not self.connected: return False
        
        # Canlı veri üret
        data = self.generate_realtime_data()
        
        # Gönder
        self.client.publish(self.publish_topic, json.dumps(data))
        return True

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

# Yardımcı Fonksiyon
def create_mqtt_sensors(num_sensors=4, broker='broker.hivemq.com', port=1883):
    nodes = []
    for i in range(1, num_sensors + 1):
        nodes.append(MQTTSensorNode(node_id=i, broker=broker, port=port))
    return nodes