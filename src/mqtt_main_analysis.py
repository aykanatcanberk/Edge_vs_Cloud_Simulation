"""
09_mqtt_main_analysis.py 
YOĞUN YÜK ALTINDA KENAR BİLİŞİM SİMÜLASYONU
(Canlı Veri + 5000 Döngü + AI Analizi)
"""

import time
import numpy as np
import matplotlib.pyplot as plt
from mqtt_sensor_simulator import create_mqtt_sensors
from mqtt_edge_device import MQTTEdgeDevice
from mqtt_cloud_platform import CloudPlatform

class MQTTSystemSimulation:
    def __init__(self, num_cycles=5000, num_sensors=4):
        self.num_cycles = num_cycles
        self.num_sensors = num_sensors
        self.broker = 'broker.hivemq.com'
        self.port = 1883
        
        # Sonuçları sakla
        self.results = {
            'edge': {'latency': [], 'bandwidth': 0, 'decisions': 0},
            'cloud': {'latency': [], 'bandwidth': 0, 'decisions': 0}
        }

    def run_scenario(self, mode='edge'):
        """
        Tek bir fonksiyonla iki senaryoyu da çalıştırır.
        mode: 'edge' veya 'cloud'
        """
        scenario_name = "KENAR BİLİŞİM (EDGE AI)" if mode == 'edge' else "GELENEKSEL BULUT"
        print("\n" + "="*70)
        print(f"SENARYO BAŞLATILIYOR: {scenario_name}")
        print(f"Hedef: {self.num_cycles} Veri Paketi | Sensör Sayısı: {self.num_sensors}")
        print("="*70)
        
        # 1. Bileşenleri Başlat
        # Not: create_mqtt_sensors artık veri seti istemiyor, canlı üretiyor.
        sensors = create_mqtt_sensors(self.num_sensors, self.broker, self.port)
        
        if mode == 'edge':
            edge = MQTTEdgeDevice(device_id='edge_sim', broker=self.broker)
            edge.connect()
            # Cloud sadece uyarıları dinler
            cloud = CloudPlatform(platform_id='cloud_listener', broker=self.broker)
            cloud.connect()
        else:
            # Cloud her şeyi dinler
            cloud = CloudPlatform(platform_id='cloud_main', broker=self.broker)
            cloud.connect()
            cloud.client.subscribe("iot/sensors/+/data")
        
        for s in sensors: s.connect()
        time.sleep(2) # Bağlantıların oturması için
        
        print("\n Yoğun veri akışı başladı...")
        start_time = time.time()
        
        processed_count = 0
        
        # --- SİMÜLASYON DÖNGÜSÜ ---
        while processed_count < self.num_cycles:
            for sensor in sensors:
                if processed_count >= self.num_cycles: break
                
                # A. Veri Üretimi ve Gönderimi
                if sensor.read_and_publish():
                    processed_count += 1
                    
                    # B. Gecikme Simülasyonu
                    if mode == 'edge':
                        # Sensör -> Edge (LAN: Hızlı, 2-10ms) + AI İşleme (3-15ms)
                        lat = np.random.uniform(2, 10) + np.random.uniform(3, 15)
                    else:
                        # Sensör -> Cloud (WAN: Yavaş, 60-200ms) + Cloud İşleme (20-50ms)
                        lat = np.random.uniform(60, 200) + np.random.uniform(20, 50)
                    
                    self.results[mode]['latency'].append(lat)
                    
                    # İlerleme Çubuğu (Her 500 veride bir)
                    if processed_count % 500 == 0:
                        print(f"   [{mode.upper()}] İlerleme: {processed_count}/{self.num_cycles} veri işlendi...")
            
            # Çok hızlı döngü (Yoğun trafik simülasyonu için sleep çok az)
            time.sleep(0.01) 
            
        total_time = time.time() - start_time
        print(f"\n✅ Senaryo Tamamlandı. Süre: {total_time:.2f} sn")
        
        # C. İstatistikleri Topla
        if mode == 'edge':
            stats = edge.get_statistics()
            self.results['edge']['bandwidth'] = stats['cloud_messages_sent']
            self.results['edge']['decisions'] = stats['local_decisions']
            self.results['edge']['ai_anomalies'] = stats.get('ai_anomalies', 0)
            edge.disconnect()
        else:
            # Cloud senaryosunda tüm veriler cloud'a gider
            self.results['cloud']['bandwidth'] = processed_count
            self.results['cloud']['decisions'] = 0 # Cloud'da yerel karar yok
        
        cloud.disconnect()
        for s in sensors: s.disconnect()
        time.sleep(2)

    def generate_report(self):
        print("\n" + "="*70)
        print("FİNAL PERFORMANS KARŞILAŞTIRMA RAPORU")
        print("="*70)
        
        e_lat = np.mean(self.results['edge']['latency'])
        c_lat = np.mean(self.results['cloud']['latency'])
        
        e_bw = self.results['edge']['bandwidth']
        c_bw = self.results['cloud']['bandwidth']
        
        print(f"\n1. HIZ (GECİKME)")
        print(f"   Kenar Bilişim: {e_lat:.2f} ms")
        print(f"   Bulut Bilişim: {c_lat:.2f} ms")
        print(f"   --> Hız Artışı: %{((c_lat-e_lat)/c_lat)*100:.1f}")
        
        print(f"\n2. BANT GENİŞLİĞİ (Veri Tasarrufu)")
        print(f"   Toplam Veri: {self.num_cycles}")
        print(f"   Buluta Gönderilen (Edge):  {e_bw}")
        print(f"   Buluta Gönderilen (Cloud): {c_bw}")
        print(f"   --> Tasarruf: %{(1 - e_bw/c_bw)*100:.1f}")
        
        print(f"\n3. YAPAY ZEKA ETKİSİ")
        print(f"   AI Tarafından Tespit Edilen Anomali: {self.results['edge'].get('ai_anomalies', 0)}")
        print(f"   Yerel Otonom Karar Sayısı: {self.results['edge']['decisions']}")
        
        # Grafik
        self.plot_results(e_lat, c_lat, e_bw, c_bw)

    def plot_results(self, el, cl, eb, cb):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Grafik 1: Gecikme
        ax1.bar(['Edge (AI)', 'Cloud'], [el, cl], color=['#2ecc71', '#e74c3c'])
        ax1.set_title('Ortalama Tepki Süresi (Düşük İyi)', fontsize=12)
        ax1.set_ylabel('Milisaniye (ms)')
        for i, v in enumerate([el, cl]):
            ax1.text(i, v, f"{v:.1f}ms", ha='center', va='bottom', fontweight='bold')
            
        # Grafik 2: Bant Genişliği
        ax2.bar(['Edge (AI)', 'Cloud'], [eb, cb], color=['#3498db', '#95a5a6'])
        ax2.set_title('Buluta Gönderilen Veri Sayısı (Düşük İyi)', fontsize=12)
        ax2.set_ylabel('Mesaj Adedi')
        for i, v in enumerate([eb, cb]):
            ax2.text(i, v, str(v), ha='center', va='bottom', fontweight='bold')
            
        plt.suptitle(f'Proje 10: Yoğun Yük Testi ({self.num_cycles} Veri Paketi)', fontsize=16)
        plt.savefig('output/final_simulation_report.png')
        print(f"\n[INFO] Grafik kaydedildi: output/final_simulation_report.png")

if __name__ == "__main__":
    # 5000 veri paketi, 4 sensör ile simülasyonu başlat
    sim = MQTTSystemSimulation(num_cycles=5000, num_sensors=4)
    
    # 1. Önce Edge Senaryosu
    sim.run_scenario(mode='edge')
    
    # 2. Sonra Cloud Senaryosu
    sim.run_scenario(mode='cloud')
    
    # 3. Raporla
    sim.generate_report()