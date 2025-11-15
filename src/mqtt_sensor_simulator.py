"""
07_mqtt_sensor_simulator.py
MQTT Protokolü ile IoT Sensör Simülasyonu
Gerçek MQTT broker'a veri gönderen sensör düğümleri
"""

import json
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import pandas as pd

class MQTTSensorNode:
    """
    MQTT protokolü kullanan IoT sensör düğümü
    ESP32 tabanlı gerçek bir sensörü simüle eder
    """
    
    def __init__(self, node_id, data_source, broker='broker.hivemq.com', port=1883):
        """
        Args:
            node_id (int): Sensör düğümü ID
            data_source (pd.DataFrame): Veri kaynağı
            broker (str): MQTT broker adresi
            port (int): MQTT broker portu
        """
        self.node_id = node_id
        self.data_source = data_source.reset_index(drop=True)
        self.current_cycle = 0
        self.total_cycles = len(data_source)
        
        # MQTT client
        client_id = f"sensor_node_{node_id}"
        self.client = mqtt.Client(client_id=client_id)
        self.broker = broker
        self.port = port
        
        # Topic
        self.publish_topic = f"iot/sensors/{node_id}/data"
        
        # Bağlantı durumu
        self.connected = False
        self.client.on_connect = self.on_connect
        
        print(f"[Sensor Node {self.node_id}] Başlatıldı - {self.total_cycles} veri noktası")
    
    def connect(self):
        """MQTT broker'a bağlan"""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            
            # Bağlantı için bekle
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            return self.connected
            
        except Exception as e:
            print(f"[Sensor Node {self.node_id}] Bağlantı hatası: {e}")
            return False
    
    def on_connect(self, client, userdata, flags, rc):
        """Bağlantı callback'i"""
        if rc == 0:
            self.connected = True
            print(f"[Sensor Node {self.node_id}] MQTT bağlantısı başarılı")
        else:
            print(f"[Sensor Node {self.node_id}] Bağlantı hatası: {rc}")
    
    def read_and_publish(self):
        """
        Sensörden veri oku ve MQTT ile yayınla
        
        Returns:
            bool: Başarılı ise True
        """
        if self.current_cycle >= self.total_cycles:
            return False
        
        if not self.connected:
            print(f"[Sensor Node {self.node_id}] Bağlantı yok!")
            return False
        
        # Veriyi oku
        row = self.data_source.iloc[self.current_cycle]
        self.current_cycle += 1
        
        # Sensör okuması oluştur
        sensor_reading = {
            'node_id': self.node_id,
            'timestamp': datetime.now().isoformat(),
            'cycle': int(row['cycle']),
            'measurements': {
                'temperature_1': float(row['sensor_temp1']),
                'temperature_2': float(row['sensor_temp2']),
                'pressure': float(row['sensor_pressure']),
                'vibration': float(row['sensor_vibration']),
                'rpm': float(row['sensor_rpm'])
            },
            'health': float(row['health_indicator']),
            'metadata': {
                'sensor_type': 'ESP32_INDUSTRIAL',
                'firmware_version': '1.2.3',
                'battery_level': 95 - (self.current_cycle * 0.1)
            }
        }
        
        # MQTT ile yayınla
        payload = json.dumps(sensor_reading)
        result = self.client.publish(self.publish_topic, payload)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            return True
        else:
            print(f"[Sensor Node {self.node_id}] Yayın hatası: {result.rc}")
            return False
    
    def disconnect(self):
        """Bağlantıyı kapat"""
        self.client.loop_stop()
        self.client.disconnect()
    
    def get_progress(self):
        """İlerleme bilgisi"""
        return {
            'node_id': self.node_id,
            'current': self.current_cycle,
            'total': self.total_cycles,
            'percentage': (self.current_cycle / self.total_cycles * 100) if self.total_cycles > 0 else 0
        }


def create_mqtt_sensors(sensor_data, num_cycles=None, broker='broker.hivemq.com', port=1883):
    """
    MQTT sensör düğümlerini oluştur
    
    Args:
        sensor_data (dict): Motor ID ve veri eşleşmeleri
        num_cycles (int): Her sensör için döngü sayısı
        broker (str): MQTT broker
        port (int): MQTT port
        
    Returns:
        list: MQTTSensorNode listesi
    """
    sensor_nodes = []
    
    for engine_id, data in sensor_data.items():
        if num_cycles:
            data = data.head(num_cycles)
        
        node = MQTTSensorNode(
            node_id=int(engine_id),
            data_source=data,
            broker=broker,
            port=port
        )
        sensor_nodes.append(node)
    
    print(f"\n✓ {len(sensor_nodes)} MQTT sensör düğümü oluşturuldu")
    return sensor_nodes


# Test fonksiyonu
def test_mqtt_sensors():
    """MQTT sensörleri test et"""
    print("\n" + "="*70)
    print("MQTT SENSÖR SİMÜLATÖRÜ TESTİ")
    print("="*70)
    
    # Veriyi yükle
    from iot_sensor_simulator import load_sensor_data
    sensor_data = load_sensor_data()
    
    if sensor_data is None:
        print("Veri seti bulunamadı!")
        return
    
    # Sensörleri oluştur (test için 3 döngü)
    nodes = create_mqtt_sensors(sensor_data, num_cycles=3)
    
    # Bağlan
    print("\nMQTT broker'a bağlanılıyor...")
    for node in nodes:
        if not node.connect():
            print(f"Node {node.node_id} bağlanamadı!")
            return
    
    time.sleep(2)  # Bağlantıların stabilize olması için
    
    # Veri gönder
    print("\nSensör verileri yayınlanıyor...")
    for cycle in range(3):
        print(f"\nDöngü {cycle + 1}/3:")
        for node in nodes:
            success = node.read_and_publish()
            if success:
                progress = node.get_progress()
                print(f"  Node {node.node_id}: {progress['current']}/{progress['total']} - "
                      f"%{progress['percentage']:.1f}")
        
        time.sleep(1)  # Mesajlar arası gecikme
    
    # Bağlantıları kapat
    print("\nBağlantılar kapatılıyor...")
    for node in nodes:
        node.disconnect()
    
    print("\n" + "="*70)
    print("✓ TEST TAMAMLANDI")
    print("="*70)
    print("\nNot: Veriler şu topic'lere gönderildi:")
    for node in nodes:
        print(f"  - iot/sensors/{node.node_id}/data")


if __name__ == "__main__":
    test_mqtt_sensors()