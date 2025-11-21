"""
09_mqtt_main_analysis.py 
MQTT Protokolü ile Kenar Bilişim vs Bulut Karşılaştırması (AI Raporlu)
"""

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# MQTT modüllerini import et
from mqtt_sensor_simulator import create_mqtt_sensors
from mqtt_edge_device import MQTTEdgeDevice
from mqtt_cloud_platform import CloudPlatform
from iot_sensor_simulator import load_sensor_data

class MQTTSystemSimulation:
    def __init__(self, num_cycles=30, broker='broker.hivemq.com', port=1883):
        self.num_cycles = num_cycles
        self.broker = broker
        self.port = port
        
        self.edge_metrics = {
            'latencies': [],
            'data_sent_to_cloud': 0,
            'total_data_points': 0,
            'anomalies_detected': 0,
            'ai_anomalies': 0,      # Yeni metrik
            'rule_anomalies': 0,    # Yeni metrik
            'local_decisions': 0
        }
        
        self.cloud_only_metrics = {
            'latencies': [],
            'data_sent_to_cloud': 0,
            'total_data_points': 0,
            'anomalies_detected': 0
        }
    
    def simulate_edge_mqtt_architecture(self):
        print("\n" + "="*70)
        print("MQTT KENAR BİLİŞİM MİMARİSİ (AI DESTEKLİ)")
        print("="*70)
        
        sensor_data = load_sensor_data()
        if sensor_data is None: return False
        
        # Bileşenler
        sensor_nodes = create_mqtt_sensors(sensor_data, num_cycles=self.num_cycles, broker=self.broker, port=self.port)
        edge_device = MQTTEdgeDevice(device_id='edge_device_mqtt', broker=self.broker, port=self.port)
        cloud = CloudPlatform(platform_id='cloud_mqtt', broker=self.broker, port=self.port)
        
        # Bağlantılar
        cloud.connect()
        time.sleep(1)
        edge_device.connect()
        time.sleep(1)
        for node in sensor_nodes: node.connect()
        time.sleep(2)
        
        print("\n--- Veri Akışı ve AI Analizi Başlıyor ---")
        
        for cycle in range(self.num_cycles):
            if cycle % 5 == 0: print(f"Döngü {cycle + 1}/{self.num_cycles} işleniyor...")
            
            for node in sensor_nodes:
                sensor_to_edge_latency = np.random.uniform(5, 15)
                if node.read_and_publish():
                    self.edge_metrics['total_data_points'] += 1
                    # AI işlem süresini simüle etmek için biraz daha uzun işlem süresi
                    edge_processing = np.random.uniform(2, 5) 
                    self.edge_metrics['latencies'].append(sensor_to_edge_latency + edge_processing)
            
            time.sleep(0.3)
        
        print("\nSonuçlar toplanıyor...")
        time.sleep(3)
        
        # İstatistikleri Al
        edge_stats = edge_device.get_statistics()
        self.edge_metrics['anomalies_detected'] = edge_stats['anomalies_detected']
        self.edge_metrics['ai_anomalies'] = edge_stats.get('ai_anomalies', 0)
        self.edge_metrics['rule_anomalies'] = edge_stats.get('rule_anomalies', 0)
        self.edge_metrics['local_decisions'] = edge_stats['local_decisions']
        self.edge_metrics['data_sent_to_cloud'] = edge_stats['cloud_messages_sent']
        
        # Kapat
        for node in sensor_nodes: node.disconnect()
        edge_device.disconnect()
        cloud.disconnect()
        
        return True
    
    def simulate_cloud_only_mqtt(self):
        # (Burası önceki kodla aynı kalabilir, kısalık için özetledim)
        print("\n" + "="*70)
        print("MQTT BULUT MERKEZLİ MİMARİ")
        print("="*70)
        sensor_data = load_sensor_data()
        sensor_nodes = create_mqtt_sensors(sensor_data, num_cycles=self.num_cycles, broker=self.broker, port=self.port)
        cloud = CloudPlatform(platform_id='cloud_direct', broker=self.broker, port=self.port)
        
        cloud.connect()
        cloud.client.subscribe("iot/sensors/+/data")
        time.sleep(1)
        for node in sensor_nodes: node.connect()
        time.sleep(2)
        
        print("Veriler doğrudan buluta gönderiliyor...")
        for cycle in range(self.num_cycles):
            for node in sensor_nodes:
                lat = np.random.uniform(50, 150) # İnternet gecikmesi
                if node.read_and_publish():
                    self.cloud_only_metrics['total_data_points'] += 1
                    self.cloud_only_metrics['data_sent_to_cloud'] += 1
                    self.cloud_only_metrics['latencies'].append(lat + 15)
            time.sleep(0.1)
            
        time.sleep(2)
        for node in sensor_nodes: node.disconnect()
        cloud.disconnect()
        return True

    def generate_comparison_report(self):
        print("\n" + "="*70)
        print("SONUÇ RAPORU: KENAR BİLİŞİM VE YAPAY ZEKA")
        print("="*70)
        
        edge_avg = np.mean(self.edge_metrics['latencies'])
        cloud_avg = np.mean(self.cloud_only_metrics['latencies'])
        
        # 1. AI PERFORMANSI (YENİ BÖLÜM)
        print("\n1. YAPAY ZEKA ve ANOMALİ ANALİZİ")
        print("-" * 70)
        print(f"Toplam Anomali:      {self.edge_metrics['anomalies_detected']}")
        print(f"🤖 AI Tarafından:    {self.edge_metrics['ai_anomalies']} (Karmaşık Desenler)")
        print(f"📏 Kural Tarafından: {self.edge_metrics['rule_anomalies']} (Sabit Eşikler)")
        print(f"⚡ Otonom Kararlar:  {self.edge_metrics['local_decisions']} (Buluta sormadan)")
        
        # 2. GECİKME
        print("\n2. GECİKME (LATENCY) ANALİZİ")
        print("-" * 70)
        print(f"Kenar Bilişim (AI):  {edge_avg:.2f} ms")
        print(f"Bulut Bilişim:       {cloud_avg:.2f} ms")
        print(f"--> Hızlanma:        %{((cloud_avg-edge_avg)/cloud_avg)*100:.1f}")

        # 3. VERİ TRAFİĞİ
        print("\n3. BANT GENİŞLİĞİ TASARRUFU")
        print("-" * 70)
        total = self.edge_metrics['total_data_points']
        sent = self.edge_metrics['data_sent_to_cloud']
        saved = (1 - sent/total) * 100
        print(f"İşlenen Veri:        {total}")
        print(f"Buluta Gönderilen:   {sent}")
        print(f"--> Tasarruf:        %{saved:.1f}")
        
        return saved

def main():
    sim = MQTTSystemSimulation(num_cycles=30)
    sim.simulate_edge_mqtt_architecture()
    sim.simulate_cloud_only_mqtt()
    sim.generate_comparison_report()

if __name__ == "__main__":
    main()