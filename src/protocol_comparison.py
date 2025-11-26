"""
10_protocol_comparison.py
 Final Analizi: Edge (MQTT) vs Cloud (MQTT) vs Cloud (HTTP)
 Canlı Veri Simülasyonu ile Uyumlu)
"""

import time
import numpy as np
import matplotlib.pyplot as plt
from mqtt_sensor_simulator import create_mqtt_sensors
from mqtt_edge_device import MQTTEdgeDevice
from mqtt_cloud_platform import CloudPlatform

class ProtocolSimulation:
    def __init__(self, num_cycles=1000, num_sensors=4):
        self.num_cycles = num_cycles
        self.num_sensors = num_sensors
        self.broker = 'broker.hivemq.com'
        self.port = 1883
        
        # Sonuçlar (3 Farklı Senaryo)
        self.results = {
            'edge_mqtt':  {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0},
            'cloud_mqtt': {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0},
            'cloud_http': {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0}
        }
        
        # Protokol Yükleri (Simülasyon Sabitleri)
        self.MQTT_HEADER_SIZE = 50   # Byte (Yaklaşık)
        self.HTTP_HEADER_SIZE = 500  # Byte (Yaklaşık - Headers, Cookies vb.)
        self.PAYLOAD_SIZE = 250      # Byte (JSON verisi)

    def run_edge_mqtt(self):
        """Senaryo 1: Kenar Bilişim + MQTT"""
        print("\n" + "="*60)
        print("SENARYO 1: KENAR BİLİŞİM (AI + MQTT)")
        print("="*60)
        
        # DÜZELTME BURADA: Artık sensor_data yok, sadece sensör sayısı veriyoruz
        nodes = create_mqtt_sensors(self.num_sensors, self.broker, self.port)
        edge = MQTTEdgeDevice()
        
        if not edge.connect(): return
        for n in nodes: n.connect()
        time.sleep(1)
        
        print(" Veri akışı başladı...")
        
        processed_count = 0
        # Kenar senaryosunda sadece ANOMALİLER buluta gider
        anomalies_sent = 0
        
        # Simülasyon Döngüsü
        while processed_count < self.num_cycles:
            for node in nodes:
                if processed_count >= self.num_cycles: break
                
                # Canlı veri üret ve gönder
                if node.read_and_publish():
                    processed_count += 1
                    
                    # Gecikme: LAN (Hızlı) + Edge AI İşleme
                    lat = np.random.uniform(2, 8) + np.random.uniform(3, 6)
                    self.results['edge_mqtt']['latency'].append(lat)
                    
                    # Bant Genişliği: Sadece anomali varsa veri gider
                    # Simülasyonda rastgele %5 anomali olduğunu varsayalım (AI'nın bulduğu)
                    if np.random.random() < 0.05:
                        anomalies_sent += 1
            
            time.sleep(0.01) # Hızlı akış
                        
        # Toplam Veri Trafiği = Gönderilen Anomali Sayısı * (Payload + MQTT Header)
        total_bytes = anomalies_sent * (self.PAYLOAD_SIZE + self.MQTT_HEADER_SIZE)
        self.results['edge_mqtt']['bandwidth_bytes'] = total_bytes
        self.results['edge_mqtt']['decisions'] = processed_count # Her veriye karar verildi
        
        print(f"✅ Tamamlandı. Buluta giden paket: {anomalies_sent}/{processed_count}")
        edge.disconnect()
        for n in nodes: n.disconnect()

    def run_cloud_mqtt(self):
        """Senaryo 2: Bulut + MQTT"""
        print("\n" + "="*60)
        print("SENARYO 2: BULUT MERKEZLİ (MQTT)")
        print("="*60)
        
        processed_count = 0
        
        # Simülasyon döngüsü
        for _ in range(self.num_cycles):
            # Gecikme: İnternet (Orta) + Bulut İşleme
            # MQTT bağlantısı sürekli açık olduğu için "Handshake" süresi yoktur
            lat = np.random.uniform(40, 100) + np.random.uniform(10, 30)
            self.results['cloud_mqtt']['latency'].append(lat)
            processed_count += 1
            
        # Bant Genişliği: Her veri paketi buluta gider
        # Toplam = Tüm Mesajlar * (Payload + MQTT Header)
        total_bytes = processed_count * (self.PAYLOAD_SIZE + self.MQTT_HEADER_SIZE)
        self.results['cloud_mqtt']['bandwidth_bytes'] = total_bytes
        print(f"✅ Tamamlandı. Buluta giden paket: {processed_count}")

    def run_cloud_http(self):
        """Senaryo 3: Bulut + HTTP (REST API)"""
        print("\n" + "="*60)
        print("SENARYO 3: BULUT MERKEZLİ (HTTP/REST)")
        print("="*60)
        print(" HTTP Handshake ve Header yükü simüle ediliyor...")
        
        processed_count = 0
        
        for _ in range(self.num_cycles):
            # Gecikme: İnternet + TCP Handshake (HTTP her istekte yeni bağlantı açabilir) + Bulut
            # HTTP, MQTT'ye göre bağlantı kurma maliyeti (overhead) ekler (+20-50ms)
            http_handshake = np.random.uniform(20, 50)
            network_lat = np.random.uniform(40, 100)
            cloud_proc = np.random.uniform(10, 30)
            
            total_lat = http_handshake + network_lat + cloud_proc
            self.results['cloud_http']['latency'].append(total_lat)
            processed_count += 1
            
        # Bant Genişliği: Her veri paketi buluta gider
        # HTTP Headerları metin tabanlıdır ve çok yer kaplar
        # Toplam = Tüm Mesajlar * (Payload + HTTP Header)
        total_bytes = processed_count * (self.PAYLOAD_SIZE + self.HTTP_HEADER_SIZE)
        self.results['cloud_http']['bandwidth_bytes'] = total_bytes
        print(f"✅ Tamamlandı. Buluta giden paket: {processed_count}")

    def generate_report(self):
        print("\n" + "="*70)
        print(f"{'FİNAL PROTOKOL VE MİMARİ KARŞILAŞTIRMASI':^70}")
        print("="*70)
        
        metrics = {
            'Edge (AI+MQTT)': self.results['edge_mqtt'],
            'Cloud (MQTT)': self.results['cloud_mqtt'],
            'Cloud (HTTP)': self.results['cloud_http']
        }
        
        print(f"{'SENARYO':<20} | {'GECİKME (Ort)':<15} | {'VERİ TRAFİĞİ (KB)':<20} | {'VERİMLİLİK'}")
        print("-" * 80)
        
        edge_bw = metrics['Edge (AI+MQTT)']['bandwidth_bytes']
        
        for name, data in metrics.items():
            lat = np.mean(data['latency'])
            bw_kb = data['bandwidth_bytes'] / 1024 # KB cinsinden
            
            # Verimlilik (Edge'e göre ne kadar kötü?)
            efficiency = "EN İYİ 🏆"
            if bw_kb > (edge_bw / 1024) and edge_bw > 0:
                kat = data['bandwidth_bytes'] / edge_bw
                efficiency = f"{kat:.1f}x daha fazla veri"
            elif edge_bw == 0:
                 efficiency = "Veri yok"
            
            print(f"{name:<20} | {lat:.2f} ms {'⚡':<7} | {bw_kb:.2f} KB {'':<11} | {efficiency}")
            
        self.plot_charts(metrics)

    def plot_charts(self, metrics):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        names = list(metrics.keys())
        latencies = [np.mean(m['latency']) for m in metrics.values()]
        bandwidths = [m['bandwidth_bytes'] / 1024 for m in metrics.values()] 
        
        colors = ['#2ecc71', '#f1c40f', '#e74c3c'] 
        
        # Grafik 1: Gecikme
        bars1 = ax1.bar(names, latencies, color=colors)
        ax1.set_title('Ortalama Tepki Süresi (Düşük İyi)')
        ax1.set_ylabel('Milisaniye (ms)')
        ax1.grid(axis='y', alpha=0.3)
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}ms', ha='center', va='bottom', fontweight='bold')

        # Grafik 2: Bant Genişliği
        bars2 = ax2.bar(names, bandwidths, color=colors)
        ax2.set_title('Ağ Trafiği Tüketimi (Düşük İyi)')
        ax2.set_ylabel('Veri Boyutu (KB)')
        ax2.grid(axis='y', alpha=0.3)
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}KB', ha='center', va='bottom', fontweight='bold')

        plt.suptitle(f' Protokol ve Mimari Karşılaştırması ({self.num_cycles} Veri Paketi)', fontsize=16)
        plt.tight_layout()
        plt.savefig('output/protocol_comparison_report.png')
        print(f"\n[INFO] Grafik kaydedildi: output/protocol_comparison_report.png")

if __name__ == "__main__":
    sim = ProtocolSimulation(num_cycles=1000, num_sensors=4)
    
    sim.run_edge_mqtt()
    sim.run_cloud_mqtt()
    sim.run_cloud_http()
    
    sim.generate_report()