"""
08_mqtt_cloud_platform.py
MQTT ile Bulut Platform Entegrasyonu
ThingsBoard benzeri bulut platformu simülasyonu
"""

import json
import time
from datetime import datetime
from collections import defaultdict
import paho.mqtt.client as mqtt

class CloudPlatform:
    """
    IoT Cloud Platform Simülasyonu
    MQTT ile veri alır, dashboard verileri üretir
    """
    
    def __init__(self, platform_id='cloud_platform_01', broker='broker.hivemq.com', port=1883):
        """
        Args:
            platform_id (str): Platform ID
            broker (str): MQTT broker
            port (int): MQTT port
        """
        self.platform_id = platform_id
        self.broker = broker
        self.port = port
        
        # MQTT client
        self.client = mqtt.Client(client_id=platform_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Subscribe topic'ler
        self.alert_topic = "iot/cloud/alerts"
        self.sensor_topic = "iot/sensors/+/data"  # Tüm sensörler
        
        # Veri depolama
        self.telemetry_data = defaultdict(list)  # node_id: [data_points]
        self.alerts = []
        self.device_status = {}
        
        # İstatistikler
        self.statistics = {
            'total_messages': 0,
            'alert_messages': 0,
            'summary_messages': 0,
            'telemetry_messages': 0,
            'critical_alerts': 0,
            'warnings': 0,
            'devices_online': 0
        }
        
        # Bağlantı durumu
        self.connected = False
        
        print(f"[{self.platform_id}] Cloud Platform başlatıldı")
    
    def connect(self):
        """MQTT broker'a bağlan"""
        try:
            print(f"[{self.platform_id}] MQTT broker'a bağlanılıyor: {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            
            # Bağlantı için bekle
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if self.connected:
                print(f"✓ Cloud Platform bağlandı")
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
            print(f"[{self.platform_id}] MQTT bağlantısı başarılı")
            
            # Topic'lere subscribe ol
            self.client.subscribe(self.alert_topic)
            print(f"  Subscribe: {self.alert_topic}")
            
            # Opsiyonel: Ham sensör verisi de al
            # self.client.subscribe(self.sensor_topic)
            # print(f"  Subscribe: {self.sensor_topic}")
        else:
            print(f"[{self.platform_id}] Bağlantı hatası: {rc}")
    
    def on_message(self, client, userdata, msg):
        """Mesaj alma callback'i"""
        try:
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            
            self.statistics['total_messages'] += 1
            
            # Mesaj tipine göre işle
            if 'cloud/alerts' in topic:
                self.process_alert(payload)
            elif 'sensors' in topic:
                self.process_telemetry(payload)
            
        except Exception as e:
            print(f"Mesaj işleme hatası: {e}")
    
    def process_alert(self, message):
        """
        Kenar cihazdan gelen uyarıyı işle
        
        Args:
            message (dict): Uyarı mesajı
        """
        alert_type = message.get('alert_type')
        
        if alert_type == 'ANOMALY':
            self.statistics['alert_messages'] += 1
            
            # Anomali detayları
            anomalies = message.get('anomalies', [])
            for anomaly in anomalies:
                severity = anomaly.get('severity', 'INFO')
                
                if severity == 'CRITICAL':
                    self.statistics['critical_alerts'] += 1
                elif severity == 'WARNING':
                    self.statistics['warnings'] += 1
            
            # Alert kaydet
            self.alerts.append({
                'timestamp': message.get('timestamp'),
                'node_id': message.get('node_id'),
                'device_id': message.get('device_id'),
                'anomalies': anomalies,
                'critical': message.get('critical', False)
            })
            
            # Dashboard için kritik uyarı göster
            if message.get('critical'):
                print(f"\n⚠️  CRITICAL ALERT - Node {message.get('node_id')}")
                for anomaly in anomalies[:2]:  # İlk 2 anomali
                    print(f"   └─ {anomaly.get('sensor')}: {anomaly.get('value')}")
        
        elif alert_type == 'SUMMARY':
            self.statistics['summary_messages'] += 1
            
            # Özet verisini kaydet
            node_id = message.get('node_id')
            self.telemetry_data[node_id].append({
                'timestamp': message.get('timestamp'),
                'health': message.get('health'),
                'measurements': message.get('measurements')
            })
        
        # Cihaz durumunu güncelle
        node_id = message.get('node_id')
        self.device_status[node_id] = {
            'last_seen': datetime.now().isoformat(),
            'status': 'ONLINE',
            'health': message.get('health')
        }
    
    def process_telemetry(self, message):
        """
        Ham telemetri verisini işle (opsiyonel)
        
        Args:
            message (dict): Sensör verisi
        """
        self.statistics['telemetry_messages'] += 1
        
        node_id = message.get('node_id')
        
        # Telemetri kaydet
        self.telemetry_data[node_id].append({
            'timestamp': message.get('timestamp'),
            'cycle': message.get('cycle'),
            'measurements': message.get('measurements'),
            'health': message.get('health')
        })
    
    def get_dashboard_data(self):
        """
        Dashboard için özet veri
        
        Returns:
            dict: Dashboard verileri
        """
        # Aktif cihaz sayısı
        active_devices = len([d for d in self.device_status.values() 
                             if d['status'] == 'ONLINE'])
        
        # Son uyarılar
        recent_alerts = sorted(self.alerts, 
                              key=lambda x: x['timestamp'], 
                              reverse=True)[:5]
        
        # Cihaz sağlık durumu
        device_health = {}
        for node_id, status in self.device_status.items():
            device_health[node_id] = {
                'health': status.get('health', 0),
                'status': status['status']
            }
        
        return {
            'statistics': self.statistics,
            'active_devices': active_devices,
            'recent_alerts': recent_alerts,
            'device_health': device_health,
            'total_telemetry_points': sum(len(v) for v in self.telemetry_data.values())
        }
    
    def print_dashboard(self):
        """Dashboard verilerini konsola yazdır"""
        dashboard = self.get_dashboard_data()
        
        print("\n" + "="*70)
        print("CLOUD PLATFORM DASHBOARD")
        print("="*70)
        
        print("\n1. GENEL İSTATİSTİKLER:")
        print(f"   Aktif Cihaz: {dashboard['active_devices']}")
        print(f"   Toplam Mesaj: {dashboard['statistics']['total_messages']}")
        print(f"   Uyarı Mesajı: {dashboard['statistics']['alert_messages']}")
        print(f"   Kritik Uyarı: {dashboard['statistics']['critical_alerts']}")
        print(f"   Uyarı: {dashboard['statistics']['warnings']}")
        print(f"   Telemetri Noktası: {dashboard['total_telemetry_points']}")
        
        print("\n2. CİHAZ SAĞLIK DURUMU:")
        for node_id, health in dashboard['device_health'].items():
            status_icon = "✅" if health['status'] == 'ONLINE' else "❌"
            health_val = health['health']
            health_icon = "🔴" if health_val < 30 else "🟡" if health_val < 60 else "🟢"
            print(f"   {status_icon} Node {node_id}: {health_icon} {health_val:.1f}%")
        
        if dashboard['recent_alerts']:
            print("\n3. SON UYARILAR:")
            for i, alert in enumerate(dashboard['recent_alerts'], 1):
                critical_icon = "🔴" if alert['critical'] else "🟡"
                print(f"   {i}. {critical_icon} Node {alert['node_id']} - "
                      f"{len(alert['anomalies'])} anomali tespit edildi")
        
        print("\n" + "="*70)
    
    def disconnect(self):
        """Bağlantıyı kapat"""
        self.client.loop_stop()
        self.client.disconnect()
        print(f"[{self.platform_id}] MQTT bağlantısı kapatıldı")
    
    def get_statistics(self):
        """İstatistikleri getir"""
        return self.statistics.copy()


# Test fonksiyonu
def test_cloud_platform():
    """Cloud platform'u test et"""
    print("\n" + "="*70)
    print("MQTT CLOUD PLATFORM TESTİ")
    print("="*70)
    
    # Cloud platform oluştur
    cloud = CloudPlatform()
    
    # Bağlan
    if not cloud.connect():
        print("Bağlantı başarısız!")
        return
    
    print("\nCloud platform uyarıları dinliyor...")
    print("Test mesajları göndermek için 07_mqtt_sensor_simulator.py çalıştırın")
    print("Veya 06_mqtt_edge_device.py ile kenar cihazdan uyarı gönderin")
    print("\n20 saniye dinleniyor...")
    
    # 20 saniye dinle
    time.sleep(20)
    
    # Dashboard göster
    cloud.print_dashboard()
    
    # Bağlantıyı kapat
    cloud.disconnect()
    
    print("\n✓ TEST TAMAMLANDI")


if __name__ == "__main__":
    test_cloud_platform()