"""
iot_sensor_simulator.py
IoT Sensör Düğümleri Simülasyonu (ESP32 Benzeri)
Bu modül bağımsız çalışmaz, 05_main_analysis.py tarafından kullanılır
"""

import json
import time
from datetime import datetime
import pandas as pd

class IoTSensorNode:
    """
    ESP32 tabanlı IoT sensör düğümü simülasyonu
    Gerçek bir sensörün davranışını taklit eder
    """
    
    def __init__(self, node_id, data_source):
        """
        Args:
            node_id (int): Sensör düğümü ID'si (1-4)
            data_source (pd.DataFrame): Sensör veri kaynağı
        """
        self.node_id = node_id
        self.data_source = data_source.reset_index(drop=True)
        self.current_cycle = 0
        self.total_cycles = len(data_source)
        
        print(f"[Sensor Node {self.node_id}] Başlatıldı - {self.total_cycles} veri noktası")
    
    def read_sensor_data(self):
        """
        Sensörden bir ölçüm al
        
        Returns:
            dict: Sensör okuması veya None (veri bittiyse)
        """
        if self.current_cycle >= self.total_cycles:
            return None
        
        row = self.data_source.iloc[self.current_cycle]
        self.current_cycle += 1
        
        # Gerçek sensör verisi formatı
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
                'battery_level': 95 - (self.current_cycle * 0.1)  # Pil azalması
            }
        }
        
        return sensor_reading
    
    def get_status(self):
        """Sensör durumunu getir"""
        return {
            'node_id': self.node_id,
            'progress': f"{self.current_cycle}/{self.total_cycles}",
            'percentage': (self.current_cycle / self.total_cycles) * 100 if self.total_cycles > 0 else 0
        }
    
    def simulate_streaming(self, num_readings=10):
        """
        Belirli sayıda ölçüm al (test amaçlı)
        
        Args:
            num_readings (int): Okunacak veri sayısı
            
        Returns:
            list: Ölçüm listesi
        """
        readings = []
        for _ in range(num_readings):
            data = self.read_sensor_data()
            if data is None:
                break
            readings.append(data)
        
        return readings


def load_sensor_data(filepath='data/turbofan_sensor_data.csv'):
    """
    Sensör veri setini yükle ve düğümlere ayır
    
    Args:
        filepath (str): Veri seti dosya yolu
        
    Returns:
        dict: {engine_id: dataframe} formatında veri
    """
    try:
        df = pd.read_csv(filepath)
        print(f"✓ Veri seti yüklendi: {len(df)} kayıt")
        
        # Her motor için ayrı veri
        sensor_data = {}
        for engine_id in df['engine_id'].unique():
            sensor_data[engine_id] = df[df['engine_id'] == engine_id].reset_index(drop=True)
        
        return sensor_data
    
    except FileNotFoundError:
        print(f"✗ Veri seti bulunamadı: {filepath}")
        print("  Önce 01_data_preparation.py scriptini çalıştırın!")
        return None


def create_sensor_nodes(sensor_data, num_cycles=None):
    """
    Tüm sensör düğümlerini oluştur
    
    Args:
        sensor_data (dict): Motor ID ve veri eşleşmeleri
        num_cycles (int): Her sensör için kullanılacak döngü sayısı (None=tümü)
        
    Returns:
        list: IoTSensorNode nesnelerinin listesi
    """
    sensor_nodes = []
    
    for engine_id, data in sensor_data.items():
        if num_cycles:
            data = data.head(num_cycles)
        
        node = IoTSensorNode(
            node_id=int(engine_id),
            data_source=data
        )
        sensor_nodes.append(node)
    
    print(f"\n✓ {len(sensor_nodes)} sensör düğümü oluşturuldu")
    return sensor_nodes


# Test fonksiyonu
def test_sensors():
    """Sensör modülünü test et"""
    print("\n" + "="*70)
    print("ADIM 2: IoT SENSÖR SİMÜLATÖRÜ TESTİ")
    print("="*70)
    
    # Veriyi yükle
    sensor_data = load_sensor_data()
    if sensor_data is None:
        return
    
    # Sensörleri oluştur (ilk 5 döngü için test)
    nodes = create_sensor_nodes(sensor_data, num_cycles=5)
    
    print("\n" + "-"*70)
    print("ÖRNEK SENSÖR OKUMASI (Node 1, İlk Ölçüm):")
    print("-"*70)
    
    # İlk sensörden örnek okuma
    sample_reading = nodes[0].read_sensor_data()
    print(json.dumps(sample_reading, indent=2, ensure_ascii=False))
    
    print("\n" + "-"*70)
    print("TÜM SENSÖRLERDEN İLK OKUMA:")
    print("-"*70)
    
    for node in nodes:
        reading = node.read_sensor_data()
        print(f"Node {reading['node_id']}: "
              f"Cycle {reading['cycle']}, "
              f"Temp1={reading['measurements']['temperature_1']:.1f}°F, "
              f"Health={reading['health']:.1f}%")
    
    print("\n" + "="*70)
    print("✓ SENSÖR SİMÜLATÖRÜ TEST EDİLDİ")
    print("="*70)
    print("\nNot: Bu modül 05_main_analysis.py tarafından kullanılacak")
    print()


if __name__ == "__main__":
    test_sensors()