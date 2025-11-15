"""
03_edge_device.py
Kenar Bilişim Cihazı (Edge Computing Device) - Raspberry Pi Simülasyonu
Sensör verilerini işler, anomali tespit eder, aktüatörleri kontrol eder
"""

import time
import statistics
from collections import deque
from datetime import datetime

class EdgeComputingDevice:
    """
    Raspberry Pi 4 benzeri kenar bilişim cihazı simülasyonu
    Görevleri:
    - Sensör verilerini al ve işle
    - Gerçek zamanlı anomali tespiti yap
    - Aktüatörlere komut gönder
    - Sadece önemli verileri buluta gönder
    """
    
    def __init__(self, device_id='edge_device_01'):
        self.device_id = device_id
        
        # Her sensör için hareketli pencere (trend analizi için)
        self.sensor_history = {}
        self.window_size = 10
        
        # Anomali tespit eşikleri
        self.thresholds = {
            'temperature_1': 550.0,   # Fahrenheit
            'temperature_2': 680.0,   # Fahrenheit
            'pressure': 16.0,         # psi
            'vibration': 0.08,        # g
            'rpm': 2450.0,            # devir/dakika
            'health': 30.0            # yüzde (düşük = kötü)
        }
        
        # Performans metrikleri
        self.metrics = {
            'total_processed': 0,
            'anomalies_detected': 0,
            'cloud_messages_sent': 0,
            'actuator_commands': 0,
            'processing_times': deque(maxlen=1000)
        }
        
        # Anomali logu
        self.anomaly_log = []
        
        print(f"[{self.device_id}] Kenar cihaz başlatıldı")
        print(f"  Eşik değerleri yüklendi")
        print(f"  Pencere boyutu: {self.window_size}")
    
    def initialize_sensor_history(self, node_id):
        """Bir sensör için geçmiş verisi başlat"""
        if node_id not in self.sensor_history:
            self.sensor_history[node_id] = {
                'temperature_1': deque(maxlen=self.window_size),
                'temperature_2': deque(maxlen=self.window_size),
                'pressure': deque(maxlen=self.window_size),
                'vibration': deque(maxlen=self.window_size),
                'rpm': deque(maxlen=self.window_size),
                'health': deque(maxlen=self.window_size)
            }
    
    def process_sensor_data(self, sensor_reading):
        """
        Sensör verisini işle
        
        Args:
            sensor_reading (dict): Sensör okuması
            
        Returns:
            dict: İşlem sonucu {anomalies, should_alert_cloud, processing_time}
        """
        start_time = time.time()
        
        node_id = sensor_reading['node_id']
        measurements = sensor_reading['measurements']
        health = sensor_reading['health']
        
        # Sensör geçmişini başlat
        self.initialize_sensor_history(node_id)
        
        # Verileri geçmişe ekle
        for key, value in measurements.items():
            self.sensor_history[node_id][key].append(value)
        self.sensor_history[node_id]['health'].append(health)
        
        # Anomali tespiti
        anomalies = self.detect_anomalies(node_id, measurements, health)
        
        # İşlem süresini kaydet
        processing_time = (time.time() - start_time) * 1000  # milisaniye
        self.metrics['processing_times'].append(processing_time)
        self.metrics['total_processed'] += 1
        
        # Buluta gönderilmeli mi?
        should_alert_cloud = (
            len(anomalies) > 0 or  # Anomali varsa
            sensor_reading['cycle'] % 10 == 0  # Veya her 10 döngüde bir özet
        )
        
        if should_alert_cloud:
            self.metrics['cloud_messages_sent'] += 1
        
        # Aktüatör kontrolü
        if len(anomalies) > 0:
            self.metrics['anomalies_detected'] += len(anomalies)
            self.control_actuators(node_id, anomalies)
        
        return {
            'anomalies': anomalies,
            'should_alert_cloud': should_alert_cloud,
            'processing_time': processing_time
        }
    
    def detect_anomalies(self, node_id, measurements, health):
        """
        Anomali tespiti algoritması
        
        Args:
            node_id (int): Sensör düğümü ID
            measurements (dict): Sensör ölçümleri
            health (float): Sağlık göstergesi
            
        Returns:
            list: Tespit edilen anomaliler
        """
        anomalies = []
        
        # 1. Eşik değeri kontrolü
        for sensor, value in measurements.items():
            if sensor in self.thresholds:
                threshold = self.thresholds[sensor]
                if value > threshold:
                    anomalies.append({
                        'type': 'threshold_exceeded',
                        'sensor': sensor,
                        'value': value,
                        'threshold': threshold,
                        'severity': 'WARNING',
                        'message': f'{sensor} eşik değerini aştı: {value:.2f} > {threshold}'
                    })
        
        # Sağlık göstergesi kontrolü (düşük = kötü)
        if health < self.thresholds['health']:
            severity = 'CRITICAL' if health < 20 else 'WARNING'
            anomalies.append({
                'type': 'low_health',
                'sensor': 'health',
                'value': health,
                'threshold': self.thresholds['health'],
                'severity': severity,
                'message': f'Düşük sağlık seviyesi: {health:.1f}%'
            })
        
        # 2. Hızlı değişim tespiti (trend analizi)
        history = self.sensor_history[node_id]
        for sensor, value in measurements.items():
            if len(history[sensor]) >= 5:
                recent_values = list(history[sensor])[-5:]
                avg = statistics.mean(recent_values)
                
                if avg > 0:
                    change_pct = abs((value - avg) / avg) * 100
                    
                    # %15'ten fazla ani değişim
                    if change_pct > 15:
                        anomalies.append({
                            'type': 'rapid_change',
                            'sensor': sensor,
                            'value': value,
                            'previous_avg': avg,
                            'change_pct': change_pct,
                            'severity': 'WARNING',
                            'message': f'{sensor} hızlı değişim: %{change_pct:.1f}'
                        })
        
        return anomalies
    
    def control_actuators(self, node_id, anomalies):
        """
        Anomali durumunda aktüatörlere komut gönder
        
        Args:
            node_id (int): Sensör düğümü ID
            anomalies (list): Tespit edilen anomaliler
        """
        for anomaly in anomalies:
            self.metrics['actuator_commands'] += 1
            
            # Anomali loguna kaydet
            self.anomaly_log.append({
                'timestamp': datetime.now().isoformat(),
                'node_id': node_id,
                'anomaly': anomaly
            })
            
            # Simülasyon: Aktüatör komutu
            # Gerçek sistemde GPIO veya MQTT ile fiziksel aktüatör kontrolü yapılır
            if anomaly['severity'] == 'CRITICAL':
                action = 'EMERGENCY_SHUTDOWN'
            elif anomaly['severity'] == 'WARNING':
                action = 'ACTIVATE_COOLING' if 'temperature' in anomaly['sensor'] else 'ALERT'
            else:
                action = 'MONITOR'
            
            # Komut kaydı
            # print(f"  [ACTUATOR CMD] Node {node_id}: {action} - {anomaly['message']}")
    
    def create_cloud_message(self, sensor_reading, anomalies):
        """
        Buluta gönderilecek mesajı oluştur (filtrelenmiş/özetlenmiş)
        
        Args:
            sensor_reading (dict): Orijinal sensör verisi
            anomalies (list): Tespit edilen anomaliler
            
        Returns:
            dict: Bulut mesajı
        """
        return {
            'device_id': self.device_id,
            'node_id': sensor_reading['node_id'],
            'timestamp': sensor_reading['timestamp'],
            'cycle': sensor_reading['cycle'],
            'has_anomaly': len(anomalies) > 0,
            'anomaly_count': len(anomalies),
            'anomalies': anomalies,
            'summary': {
                'health': sensor_reading['health'],
                'temperature_avg': (
                    sensor_reading['measurements']['temperature_1'] + 
                    sensor_reading['measurements']['temperature_2']
                ) / 2,
                'critical_status': any(a['severity'] == 'CRITICAL' for a in anomalies)
            }
        }
    
    def get_statistics(self):
        """Performans istatistiklerini getir"""
        stats = self.metrics.copy()
        
        if self.metrics['processing_times']:
            stats['avg_processing_time'] = statistics.mean(self.metrics['processing_times'])
            stats['max_processing_time'] = max(self.metrics['processing_times'])
            stats['min_processing_time'] = min(self.metrics['processing_times'])
        else:
            stats['avg_processing_time'] = 0
            stats['max_processing_time'] = 0
            stats['min_processing_time'] = 0
        
        # Veri azaltma oranı
        if stats['total_processed'] > 0:
            stats['data_reduction_pct'] = (
                1 - stats['cloud_messages_sent'] / stats['total_processed']
            ) * 100
        else:
            stats['data_reduction_pct'] = 0
        
        return stats


# Test fonksiyonu
def test_edge_device():
    """Kenar cihazı test et"""
    print("\n" + "="*70)
    print("ADIM 3: KENAR BİLİŞİM CİHAZI TESTİ")
    print("="*70)
    
    # Kenar cihaz oluştur
    edge = EdgeComputingDevice()
    
    # Test verisi (örnek sensör okuması)
    test_reading = {
        'node_id': 1,
        'timestamp': datetime.now().isoformat(),
        'cycle': 150,
        'measurements': {
            'temperature_1': 555.0,  # Eşiği aşıyor!
            'temperature_2': 685.0,  # Eşiği aşıyor!
            'pressure': 15.8,
            'vibration': 0.075,
            'rpm': 2420.0
        },
        'health': 25.0  # Düşük sağlık!
    }
    
    print("\n" + "-"*70)
    print("TEST VERİSİ:")
    print("-"*70)
    print(f"Node ID: {test_reading['node_id']}")
    print(f"Cycle: {test_reading['cycle']}")
    print(f"Temperature 1: {test_reading['measurements']['temperature_1']}°F")
    print(f"Temperature 2: {test_reading['measurements']['temperature_2']}°F")
    print(f"Health: {test_reading['health']}%")
    
    # Veriyi işle
    result = edge.process_sensor_data(test_reading)
    
    print("\n" + "-"*70)
    print("İŞLEM SONUCU:")
    print("-"*70)
    print(f"İşlem süresi: {result['processing_time']:.2f} ms")
    print(f"Anomali sayısı: {len(result['anomalies'])}")
    print(f"Buluta gönderilmeli: {result['should_alert_cloud']}")
    
    if result['anomalies']:
        print("\nTESPİT EDİLEN ANOMALİLER:")
        for i, anomaly in enumerate(result['anomalies'], 1):
            print(f"  {i}. [{anomaly['severity']}] {anomaly['message']}")
    
    # İstatistikler
    stats = edge.get_statistics()
    print("\n" + "-"*70)
    print("KENAR CİHAZ İSTATİSTİKLERİ:")
    print("-"*70)
    print(f"İşlenen veri: {stats['total_processed']}")
    print(f"Tespit edilen anomali: {stats['anomalies_detected']}")
    print(f"Aktüatör komutu: {stats['actuator_commands']}")
    print(f"Ortalama işlem süresi: {stats['avg_processing_time']:.2f} ms")
    
    print("\n" + "="*70)
    print("✓ KENAR CİHAZ TEST EDİLDİ")
    print("="*70)
    print("\nNot: Bu modül 05_main_analysis.py tarafından kullanılacak")
    print()


if __name__ == "__main__":
    test_edge_device()