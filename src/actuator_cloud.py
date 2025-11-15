"""
04_actuator_cloud.py
Aktüatör ve Bulut Sunucu Simülasyonları
Bu modül 05_main_analysis.py tarafından kullanılır
"""

from datetime import datetime

class IoTActuator:
    """
    IoT Aktüatör simülasyonu (Alarm, Fan, Valf vb.)
    Kenar cihazdan gelen komutları çalıştırır
    """
    
    def __init__(self, actuator_id, actuator_type='alarm'):
        """
        Args:
            actuator_id (int): Aktüatör ID
            actuator_type (str): Aktüatör tipi (alarm, fan, valve, motor vb.)
        """
        self.actuator_id = actuator_id
        self.actuator_type = actuator_type
        self.state = 'OFF'
        self.activation_count = 0
        self.activation_log = []
    
    def activate(self, reason, severity='INFO'):
        """
        Aktüatörü aktif et
        
        Args:
            reason (str): Aktivasyon sebebi
            severity (str): Önem derecesi (INFO, WARNING, CRITICAL)
        """
        self.state = 'ON'
        self.activation_count += 1
        
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'reason': reason,
            'severity': severity,
            'action': f'{self.actuator_type.upper()}_ACTIVATED'
        }
        
        self.activation_log.append(log_entry)
        
        # Simülasyon çıktısı (gerçek sistemde GPIO sinyali gönderilir)
        # print(f"  [ACTUATOR {self.actuator_id}] {self.actuator_type.upper()} "
        #       f"activated - {reason}")
        
        return log_entry
    
    def deactivate(self):
        """Aktüatörü kapat"""
        if self.state == 'ON':
            self.state = 'OFF'
            # print(f"  [ACTUATOR {self.actuator_id}] Deactivated")
    
    def get_status(self):
        """Aktüatör durumunu getir"""
        return {
            'actuator_id': self.actuator_id,
            'type': self.actuator_type,
            'state': self.state,
            'activation_count': self.activation_count,
            'total_activations': len(self.activation_log)
        }


class CloudServer:
    """
    Bulut Sunucu Simülasyonu
    Kenar cihazlardan gelen verileri alır ve depolar
    """
    
    def __init__(self, server_id='cloud_01'):
        self.server_id = server_id
        self.message_storage = []
        self.statistics = {
            'total_messages': 0,
            'anomaly_messages': 0,
            'normal_messages': 0,
            'critical_alerts': 0,
            'warnings': 0
        }
    
    def receive_message(self, message):
        """
        Kenar cihazdan mesaj al
        
        Args:
            message (dict): Kenar cihazdan gelen mesaj
        """
        self.message_storage.append({
            'received_at': datetime.now().isoformat(),
            'message': message
        })
        
        self.statistics['total_messages'] += 1
        
        if message.get('has_anomaly'):
            self.statistics['anomaly_messages'] += 1
            
            # Anomali şiddetine göre say
            for anomaly in message.get('anomalies', []):
                if anomaly.get('severity') == 'CRITICAL':
                    self.statistics['critical_alerts'] += 1
                elif anomaly.get('severity') == 'WARNING':
                    self.statistics['warnings'] += 1
        else:
            self.statistics['normal_messages'] += 1
    
    def get_statistics(self):
        """Bulut sunucu istatistiklerini getir"""
        return self.statistics.copy()
    
    def get_recent_messages(self, count=10):
        """Son N mesajı getir"""
        return self.message_storage[-count:]


def create_actuators(num_actuators=4):
    """
    Aktüatör sistemi oluştur
    
    Args:
        num_actuators (int): Oluşturulacak aktüatör sayısı
        
    Returns:
        dict: {actuator_id: IoTActuator} eşleşmesi
    """
    actuators = {}
    
    actuator_types = ['alarm_system', 'cooling_fan', 'emergency_valve', 'notification_light']
    
    for i in range(1, num_actuators + 1):
        actuator_type = actuator_types[(i-1) % len(actuator_types)]
        actuators[i] = IoTActuator(
            actuator_id=i,
            actuator_type=actuator_type
        )
    
    print(f"✓ {num_actuators} aktüatör oluşturuldu")
    return actuators


# Test fonksiyonu
def test_components():
    """Aktüatör ve bulut sunucu bileşenlerini test et"""
    print("\n" + "="*70)
    print("ADIM 4: AKTÜATÖR VE BULUT SUNUCU TESTİ")
    print("="*70)
    
    # 1. Aktüatör testi
    print("\n1. AKTÜATÖR TESTİ:")
    print("-" * 70)
    
    actuator = IoTActuator(actuator_id=1, actuator_type='cooling_fan')
    print(f"Aktüatör oluşturuldu: ID={actuator.actuator_id}, Tip={actuator.actuator_type}")
    
    # Aktivasyon
    actuator.activate(reason="Yüksek sıcaklık tespit edildi", severity="WARNING")
    print(f"Durum: {actuator.state}")
    print(f"Aktivasyon sayısı: {actuator.activation_count}")
    
    actuator.deactivate()
    print(f"Durum: {actuator.state}")
    
    # 2. Bulut sunucu testi
    print("\n2. BULUT SUNUCU TESTİ:")
    print("-" * 70)
    
    cloud = CloudServer()
    print(f"Bulut sunucu oluşturuldu: ID={cloud.server_id}")
    
    # Test mesajları
    test_messages = [
        {
            'node_id': 1,
            'has_anomaly': False,
            'summary': {'health': 85.0}
        },
        {
            'node_id': 2,
            'has_anomaly': True,
            'anomalies': [
                {'severity': 'WARNING', 'message': 'Yüksek sıcaklık'}
            ]
        },
        {
            'node_id': 3,
            'has_anomaly': True,
            'anomalies': [
                {'severity': 'CRITICAL', 'message': 'Kritik arıza'}
            ]
        }
    ]
    
    for msg in test_messages:
        cloud.receive_message(msg)
    
    stats = cloud.get_statistics()
    print(f"\nBulut İstatistikleri:")
    print(f"  Toplam mesaj: {stats['total_messages']}")
    print(f"  Anomali mesajı: {stats['anomaly_messages']}")
    print(f"  Normal mesaj: {stats['normal_messages']}")
    print(f"  Kritik uyarı: {stats['critical_alerts']}")
    print(f"  Uyarı: {stats['warnings']}")
    
    print("\n" + "="*70)
    print("✓ AKTÜATÖR VE BULUT SUNUCU TEST EDİLDİ")
    print("="*70)
    print("\nNot: Bu bileşenler 05_main_analysis.py tarafından kullanılacak")
    print()


if __name__ == "__main__":
    test_components()