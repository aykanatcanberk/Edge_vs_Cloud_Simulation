"""
09_mqtt_main_analysis.py 
MQTT Protokolü ile Kenar Bilişim vs Bulut Karşılaştırması (Doğruluk Analizli)
"""

import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Modülleri import et
from mqtt_sensor_simulator import create_mqtt_sensors
from mqtt_edge_device import MQTTEdgeDevice
from mqtt_cloud_platform import CloudPlatform
from iot_sensor_simulator import load_sensor_data

class MQTTSystemSimulation:
    def __init__(self, num_cycles=30, broker='broker.hivemq.com', port=1883):
        self.num_cycles = num_cycles
        self.broker = broker
        self.port = port
        
        # Gerçek zamanlı doğruluk takibi için değişkenler
        self.accuracy_metrics = {
            'true_positives': 0,  # Arızayı bildi
            'false_positives': 0, # Sağlam motora arıza dedi
            'true_negatives': 0,  # Sağlamı bildi
            'false_negatives': 0  # Arızayı kaçırdı
        }
        
        self.edge_metrics = {
            'latencies': [],
            'data_sent_to_cloud': 0,
            'total_data_points': 0,
            'anomalies_detected': 0,
            'ai_anomalies': 0,
            'rule_anomalies': 0,
            'local_decisions': 0
        }
        
        self.cloud_only_metrics = {
            'latencies': [],
            'data_sent_to_cloud': 0,
            'total_data_points': 0,
            'anomalies_detected': 0
        }
    
    def evaluate_prediction(self, node, is_anomaly_detected):
        """
        Simülasyon sırasında modelin kararını doğrula
        Varsayım: Sağlık < 60 ise gerçek bir arızadır.
        """
        # Sensörün o anki gerçek sağlık durumu (Veri setinden okuyoruz)
        try:
            current_row = node.data_source.iloc[node.current_cycle - 1]
            actual_health = float(current_row['health_indicator'])
            is_actual_failure = actual_health <= 60.0
            
            if is_actual_failure and is_anomaly_detected:
                self.accuracy_metrics['true_positives'] += 1
            elif not is_actual_failure and not is_anomaly_detected:
                self.accuracy_metrics['true_negatives'] += 1
            elif not is_actual_failure and is_anomaly_detected:
                self.accuracy_metrics['false_positives'] += 1
            elif is_actual_failure and not is_anomaly_detected:
                self.accuracy_metrics['false_negatives'] += 1
        except:
            pass

    def simulate_edge_mqtt_architecture(self):
        print("\n" + "="*70)
        print("MQTT KENAR BİLİŞİM MİMARİSİ (AI DOĞRULUK TESTLİ)")
        print("="*70)
        
        sensor_data = load_sensor_data()
        if sensor_data is None: return False
        
        sensor_nodes = create_mqtt_sensors(sensor_data, num_cycles=self.num_cycles, broker=self.broker, port=self.port)
        edge_device = MQTTEdgeDevice(device_id='edge_device_mqtt', broker=self.broker, port=self.port)
        cloud = CloudPlatform(platform_id='cloud_mqtt', broker=self.broker, port=self.port)
        
        cloud.connect(); time.sleep(1)
        edge_device.connect(); time.sleep(1)
        for node in sensor_nodes: node.connect()
        time.sleep(2)
        
        print("\n--- Veri Akışı ve Gerçek Zamanlı Analiz ---")
        
        for cycle in range(self.num_cycles):
            if cycle % 10 == 0: print(f"Döngü {cycle + 1}/{self.num_cycles} işleniyor...")
            
            for node in sensor_nodes:
                # 1. Önceki anomali sayısını kaydet
                prev_anomalies = edge_device.metrics['ai_anomalies'] + edge_device.metrics['rule_anomalies']
                
                # 2. Veriyi gönder
                success = node.read_and_publish()
                
                # 3. Gecikme simülasyonu
                time.sleep(0.1) 
                
                if success:
                    self.edge_metrics['total_data_points'] += 1
                    sensor_to_edge_latency = np.random.uniform(5, 15)
                    edge_processing = np.random.uniform(2, 5)
                    self.edge_metrics['latencies'].append(sensor_to_edge_latency + edge_processing)
                    
                    # 4. Model o an anomali buldu mu?
                    curr_anomalies = edge_device.metrics['ai_anomalies'] + edge_device.metrics['rule_anomalies']
                    is_detected = curr_anomalies > prev_anomalies
                    
                    # 5. Doğruluk kontrolü yap
                    self.evaluate_prediction(node, is_detected)
            
        print("\nSonuçlar toplanıyor...")
        time.sleep(2)
        
        edge_stats = edge_device.get_statistics()
        self.edge_metrics.update(edge_stats)
        
        for node in sensor_nodes: node.disconnect()
        edge_device.disconnect()
        cloud.disconnect()
        return True
    
    def simulate_cloud_only_mqtt(self):
        print("\n" + "="*70)
        print("MQTT BULUT MERKEZLİ MİMARİ")
        print("="*70)
        sensor_data = load_sensor_data()
        sensor_nodes = create_mqtt_sensors(sensor_data, num_cycles=self.num_cycles, broker=self.broker, port=self.port)
        cloud = CloudPlatform(platform_id='cloud_direct', broker=self.broker, port=self.port)
        
        cloud.connect()
        cloud.client.subscribe("iot/sensors/+/data")
        time.sleep(2)
        for node in sensor_nodes: node.connect()
        
        print("Veriler doğrudan buluta gönderiliyor...")
        for cycle in range(self.num_cycles):
            for node in sensor_nodes:
                if node.read_and_publish():
                    self.cloud_only_metrics['total_data_points'] += 1
                    self.cloud_only_metrics['data_sent_to_cloud'] += 1
                    self.cloud_only_metrics['latencies'].append(np.random.uniform(50, 150))
            time.sleep(0.05)
            
        time.sleep(2)
        for node in sensor_nodes: node.disconnect()
        cloud.disconnect()
        return True

    def generate_comparison_report(self):
        print("\n" + "="*70)
        print("FİNAL PERFORMANS RAPORU")
        print("="*70)
        
        # Doğruluk Hesaplama
        tp = self.accuracy_metrics['true_positives']
        tn = self.accuracy_metrics['true_negatives']
        fp = self.accuracy_metrics['false_positives']
        fn = self.accuracy_metrics['false_negatives']
        total_predictions = tp + tn + fp + fn
        
        accuracy = ((tp + tn) / total_predictions * 100) if total_predictions > 0 else 0
        precision = (tp / (tp + fp) * 100) if (tp + fp) > 0 else 0
        recall = (tp / (tp + fn) * 100) if (tp + fn) > 0 else 0
        
        print("\n1. YAPAY ZEKA DOĞRULUK ANALİZİ (Ground Truth: Health < 60)")
        print("-" * 70)
        print(f"Toplam İşlem:        {total_predictions}")
        print(f"Doğru Tespitler:     {tp} (Gerçek Arıza)")
        print(f"Yanlış Alarmlar:     {fp} (False Positive)")
        print(f"Kaçırılan Arızalar:  {fn} (False Negative)")
        print("-" * 70)
        print(f" MODEL DOĞRULUĞU (ACCURACY):  %{accuracy:.2f}")
        print(f" HASSASİYET (PRECISION):      %{precision:.2f}")
        print(f" YAKALAMA (RECALL):           %{recall:.2f}")

        edge_avg = np.mean(self.edge_metrics['latencies'])
        cloud_avg = np.mean(self.cloud_only_metrics['latencies'])
        
        print("\n2. SİSTEM PERFORMANSI")
        print("-" * 70)
        print(f"Kenar Gecikmesi:     {edge_avg:.2f} ms")
        print(f"Bulut Gecikmesi:     {cloud_avg:.2f} ms")
        print(f"Veri Tasarrufu:      %{(1 - self.edge_metrics['data_sent_to_cloud']/self.edge_metrics['total_data_points'])*100:.1f}")
        
        return accuracy

def main():
    sim = MQTTSystemSimulation(num_cycles=30)
    sim.simulate_edge_mqtt_architecture()
    sim.simulate_cloud_only_mqtt()
    sim.generate_comparison_report()

if __name__ == "__main__":
    main()