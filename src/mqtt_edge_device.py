"""
06_mqtt_edge_device.py
MQTT Protokolü ile Kenar Bilişim Cihazı
Gerçek MQTT broker'a bağlanır ve veri işler
"""

import json
import time
import statistics
from collections import deque
from datetime import datetime
import paho.mqtt.client as mqtt

class MQTTEdgeDevice:
    """
    MQTT protokolü kullanan kenar bilişim cihazı
    - Sensörlerden MQTT ile veri alır
    - Yerel işleme yapar
    - Sadece önemli verileri cloud'a gönderir
    """
    
    def __init__(self, device_id='edge_device_01', broker='broker.hivemq.com', port=1883):
        """
        Args:
            device_id (str): Cihaz ID
            broker (str): MQTT broker adresi
            port (int): MQTT broker portu
        """
        self.device_id = device_id
        self.broker = broker
        self.port = port
        
        # MQTT client
        self.client = mqtt.Client(client_id=device_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Topic'ler
        self.sensor_topic = "iot/sensors/+/data"  # Sensör verisi
        self.cloud_topic = "iot/cloud/alerts"     # Buluta gönderilen uyarılar
        self.actuator_topic = "iot/actuators/+/command"  # Aktüatör komutları
        
        # Veri işleme
        self.sensor_history = {}
        self.window_size = 10
        
        # Eşik değerleri
        self.thresholds = {
            'temperature_1': 550.0,
            'temperature_2': 680.0,
            'pressure': 16.0,
            'vibration': 0.08,
            'rpm': 2450.0,
            'health': 30.0
        }
        
        # Metrikler
        self.metrics = {
            'total_received': 0,
            'anomalies_detected': 0,
            'cloud_messages_sent': 0,
            'local_decisions': 0,
            'processing_times': deque(maxlen=1000)
        }
        
        # Anomali logu
        self.anomaly_log = []
        
        # Bağlantı durumu
        self.connected = False
        
    def connect(self):
        """MQTT broker'a bağlan"""
        try:
            print(f"[{self.device_id}] MQTT broker'a bağlanılıyor: {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            
            # Bağlantı için bekle
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if self.connected:
                print(f"✓ MQTT broker'a bağlandı")
                return True
            else:
                print(f"✗ Bağlantı zaman aşımı")
                return False
                
        except Exception as e:
            print(f"✗ Bağlantı hatası: {e}")
            return False
    
    def on_connect(self, client, userdata, flags, rc):
        """Bağlantı callback'i"""
        if rc == 0:
            self.connected = True
            print(f"[{self.device_id}] MQTT bağlantısı başarılı")
            
            # Sensör topic'ine subscribe ol
            self.client.subscribe(self.sensor_topic)
            print(f"  Subscribe: {self.sensor_topic}")
        else:
            print(f"[{self.device_id}] Bağlantı hatası: {rc}")
    
    def on_message(self, client, userdata, msg):
        """Mesaj alma callback'i"""
        try:
            # Veriyi parse et
            payload = json.loads(msg.payload.decode())
            
            # İşle
            self.process_sensor_data(payload, msg.topic)
            
        except Exception as e:
            print(f"Mesaj işleme hatası: {e}")
    
    def process_sensor_data(self, sensor_data, topic):
        """
        Gelen sensör verisini işle
        
        Args:
            sensor_data (dict): Sensör verisi
            topic (str): MQTT topic
        """
        start_time = time.time()
        
        self.metrics['total_received'] += 1
        node_id = sensor_data.get('node_id')
        
        # Sensör geçmişini başlat
        if node_id not in self.sensor_history:
            self.sensor_history[node_id] = {
                'temperature_1': deque(maxlen=self.window_size),
                'temperature_2': deque(maxlen=self.window_size),
                'pressure': deque(maxlen=self.window_size),
                'vibration': deque(maxlen=self.window_size),
                'rpm': deque(maxlen=self.window_size),
                'health': deque(maxlen=self.window_size)
            }
        
        # Verileri geçmişe ekle
        measurements = sensor_data.get('measurements', {})
        health = sensor_data.get('health', 100)
        
        for key, value in measurements.items():
            if key in self.sensor_history[node_id]:
                self.sensor_history[node_id][key].append(value)
        self.sensor_history[node_id]['health'].append(health)
        
        # Anomali tespiti
        anomalies = self.detect_anomalies(node_id, measurements, health)
        
        # İşlem süresi
        processing_time = (time.time() - start_time) * 1000
        self.metrics['processing_times'].append(processing_time)
        
        # Yerel karar: Aktüatör kontrolü
        if anomalies:
            self.metrics['anomalies_detected'] += len(anomalies)
            self.control_actuators(node_id, anomalies)
            self.metrics['local_decisions'] += 1
            
            # Buluta bildir
            self.send_to_cloud(sensor_data, anomalies)
        
        # Periyodik özet gönder (her 10 mesajda bir)
        elif self.metrics['total_received'] % 10 == 0:
            self.send_summary_to_cloud(sensor_data)
    
    def detect_anomalies(self, node_id, measurements, health):
        """Anomali tespit et"""
        anomalies = []
        
        # Eşik kontrolü
        for sensor, value in measurements.items():
            if sensor in self.thresholds and value > self.thresholds[sensor]:
                anomalies.append({
                    'type': 'threshold_exceeded',
                    'sensor': sensor,
                    'value': value,
                    'threshold': self.thresholds[sensor],
                    'severity': 'WARNING'
                })
        
        # Sağlık kontrolü
        if health < self.thresholds['health']:
            severity = 'CRITICAL' if health < 20 else 'WARNING'
            anomalies.append({
                'type': 'low_health',
                'sensor': 'health',
                'value': health,
                'threshold': self.thresholds['health'],
                'severity': severity
            })
        
        # Hızlı değişim tespiti
        history = self.sensor_history[node_id]
        for sensor, value in measurements.items():
            if sensor in history and len(history[sensor]) >= 5:
                recent = list(history[sensor])[-5:]
                avg = statistics.mean(recent)
                
                if avg > 0:
                    change_pct = abs((value - avg) / avg) * 100
                    if change_pct > 15:
                        anomalies.append({
                            'type': 'rapid_change',
                            'sensor': sensor,
                            'value': value,
                            'change_pct': change_pct,
                            'severity': 'WARNING'
                        })
        
        return anomalies
    
    def control_actuators(self, node_id, anomalies):
        """
        Aktüatörlere komut gönder (MQTT ile)
        
        Args:
            node_id (int): Sensör ID
            anomalies (list): Tespit edilen anomaliler
        """
        for anomaly in anomalies:
            # Komut oluştur
            if anomaly['severity'] == 'CRITICAL':
                action = 'EMERGENCY_SHUTDOWN'
            elif 'temperature' in anomaly.get('sensor', ''):
                action = 'ACTIVATE_COOLING'
            else:
                action = 'ALERT'
            
            # MQTT ile gönder
            actuator_topic = f"iot/actuators/{node_id}/command"
            command = {
                'timestamp': datetime.now().isoformat(),
                'node_id': node_id,
                'action': action,
                'reason': anomaly.get('sensor', 'unknown'),
                'severity': anomaly['severity']
            }
            
            self.client.publish(actuator_topic, json.dumps(command))
            
            # Log
            self.anomaly_log.append({
                'timestamp': datetime.now().isoformat(),
                'node_id': node_id,
                'anomaly': anomaly,
                'action': action
            })
    
    def send_to_cloud(self, sensor_data, anomalies):
        """
        Buluta uyarı gönder (MQTT ile)
        
        Args:
            sensor_data (dict): Sensör verisi
            anomalies (list): Anomaliler
        """
        cloud_message = {
            'device_id': self.device_id,
            'timestamp': datetime.now().isoformat(),
            'node_id': sensor_data.get('node_id'),
            'cycle': sensor_data.get('cycle'),
            'alert_type': 'ANOMALY',
            'anomalies': anomalies,
            'health': sensor_data.get('health'),
            'critical': any(a['severity'] == 'CRITICAL' for a in anomalies)
        }
        
        self.client.publish(self.cloud_topic, json.dumps(cloud_message))
        self.metrics['cloud_messages_sent'] += 1
    
    def send_summary_to_cloud(self, sensor_data):
        """Buluta özet gönder"""
        summary = {
            'device_id': self.device_id,
            'timestamp': datetime.now().isoformat(),
            'node_id': sensor_data.get('node_id'),
            'alert_type': 'SUMMARY',
            'health': sensor_data.get('health'),
            'measurements': sensor_data.get('measurements')
        }
        
        self.client.publish(self.cloud_topic, json.dumps(summary))
        self.metrics['cloud_messages_sent'] += 1
    
    def publish_sensor_data(self, sensor_reading):
        """
        Sensör verisini yayınla (simülasyon için)
        
        Args:
            sensor_reading (dict): Sensör okuması
        """
        node_id = sensor_reading['node_id']
        topic = f"iot/sensors/{node_id}/data"
        
        self.client.publish(topic, json.dumps(sensor_reading))
    
    def disconnect(self):
        """MQTT bağlantısını kapat"""
        self.client.loop_stop()
        self.client.disconnect()
        print(f"[{self.device_id}] MQTT bağlantısı kapatıldı")
    
    def get_statistics(self):
        """İstatistikleri getir"""
        stats = self.metrics.copy()
        
        if self.metrics['processing_times']:
            stats['avg_processing_time'] = statistics.mean(self.metrics['processing_times'])
        else:
            stats['avg_processing_time'] = 0
        
        if stats['total_received'] > 0:
            stats['data_reduction_pct'] = (
                1 - stats['cloud_messages_sent'] / stats['total_received']
            ) * 100
        else:
            stats['data_reduction_pct'] = 0
        
        return stats


# Test fonksiyonu
def test_mqtt_edge():
    """MQTT kenar cihazı test et"""
    print("\n" + "="*70)
    print("MQTT KENAR CİHAZ TESTİ")
    print("="*70)
    
    # Kenar cihaz oluştur
    edge = MQTTEdgeDevice()
    
    # Bağlan
    if not edge.connect():
        print("Bağlantı başarısız! Test sonlandırılıyor.")
        return
    
    # Test verisi gönder
    print("\nTest verisi gönderiliyor...")
    
    test_data = {
        'node_id': 1,
        'timestamp': datetime.now().isoformat(),
        'cycle': 100,
        'measurements': {
            'temperature_1': 555.0,  # Eşiği aşıyor
            'temperature_2': 685.0,  # Eşiği aşıyor
            'pressure': 15.5,
            'vibration': 0.075,
            'rpm': 2420.0
        },
        'health': 25.0  # Düşük!
    }
    
    edge.publish_sensor_data(test_data)
    
    # İşlenmesi için bekle
    time.sleep(2)
    
    # İstatistikler
    stats = edge.get_statistics()
    print("\nİSTATİSTİKLER:")
    print(f"  Alınan mesaj: {stats['total_received']}")
    print(f"  Tespit edilen anomali: {stats['anomalies_detected']}")
    print(f"  Yerel karar: {stats['local_decisions']}")
    print(f"  Buluta gönderilen: {stats['cloud_messages_sent']}")
    print(f"  Veri azaltma: %{stats['data_reduction_pct']:.1f}")
    
    # Bağlantıyı kapat
    edge.disconnect()
    
    print("\n" + "="*70)
    print("✓ TEST TAMAMLANDI")
    print("="*70)


if __name__ == "__main__":
    test_mqtt_edge()