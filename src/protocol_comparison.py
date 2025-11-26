"""
10_protocol_comparison.py
 Edge (MQTT) vs Cloud (MQTT) vs Cloud (HTTP)
(GELİŞMİŞ GÖRSELLEŞTİRME VERSİYONU: Boxplot, Dağılım ve Tasarruf Analizi)
"""

import time
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from mqtt_sensor_simulator import create_mqtt_sensors
from mqtt_edge_device import MQTTEdgeDevice
from mqtt_cloud_platform import CloudPlatform

class ProtocolSimulation:
    def __init__(self, num_cycles=1000, num_sensors=4):
        self.num_cycles = num_cycles
        self.num_sensors = num_sensors
        self.broker = 'broker.hivemq.com'
        self.port = 1883
        
        # Sonuçlar
        self.results = {
            'edge_mqtt':  {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0},
            'cloud_mqtt': {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0},
            'cloud_http': {'latency': [], 'bandwidth_bytes': 0, 'decisions': 0}
        }
        
        # Protokol Yükleri
        self.MQTT_HEADER_SIZE = 50
        self.HTTP_HEADER_SIZE = 500
        self.PAYLOAD_SIZE = 250

    def run_edge_mqtt(self):
        print("\n" + "="*60)
        print("SENARYO 1: KENAR BİLİŞİM (AI + MQTT)")
        print("="*60)
        
        nodes = create_mqtt_sensors(self.num_sensors, self.broker, self.port)
        edge = MQTTEdgeDevice()
        
        if not edge.connect(): return
        for n in nodes: n.connect()
        time.sleep(1)
        
        print(" Veri akışı başladı...")
        processed_count = 0
        anomalies_sent = 0
        
        while processed_count < self.num_cycles:
            for node in nodes:
                if processed_count >= self.num_cycles: break
                
                if node.read_and_publish():
                    processed_count += 1
                    # Gecikme: LAN (2-8ms) + AI İşleme (3-6ms)
                    lat = np.random.uniform(2, 8) + np.random.uniform(3, 6)
                    self.results['edge_mqtt']['latency'].append(lat)
                    
                    # Sadece anomali (%5 ihtimal) buluta gider
                    if np.random.random() < 0.05:
                        anomalies_sent += 1
            time.sleep(0.001) # Hızlı simülasyon
                        
        total_bytes = anomalies_sent * (self.PAYLOAD_SIZE + self.MQTT_HEADER_SIZE)
        self.results['edge_mqtt']['bandwidth_bytes'] = total_bytes
        
        print(f"✅ Tamamlandı. Buluta giden paket: {anomalies_sent}/{processed_count}")
        edge.disconnect()
        for n in nodes: n.disconnect()

    def run_cloud_mqtt(self):
        print("\n" + "="*60)
        print("SENARYO 2: BULUT MERKEZLİ (MQTT)")
        print("="*60)
        
        processed_count = 0
        for _ in range(self.num_cycles):
            # Gecikme: İnternet (40-100ms) + Bulut İşleme (10-30ms)
            lat = np.random.uniform(40, 100) + np.random.uniform(10, 30)
            self.results['cloud_mqtt']['latency'].append(lat)
            processed_count += 1
            
        total_bytes = processed_count * (self.PAYLOAD_SIZE + self.MQTT_HEADER_SIZE)
        self.results['cloud_mqtt']['bandwidth_bytes'] = total_bytes
        print(f"✅ Tamamlandı. Buluta giden paket: {processed_count}")

    def run_cloud_http(self):
        print("\n" + "="*60)
        print("SENARYO 3: BULUT MERKEZLİ (HTTP/REST)")
        print("="*60)
        
        processed_count = 0
        for _ in range(self.num_cycles):
            # Gecikme: Handshake (20-50ms) + İnternet + Bulut
            lat = np.random.uniform(20, 40) + np.random.uniform(40, 80) + np.random.uniform(10, 30)
            self.results['cloud_http']['latency'].append(lat)
            processed_count += 1
            
        total_bytes = processed_count * (self.PAYLOAD_SIZE + self.HTTP_HEADER_SIZE)
        self.results['cloud_http']['bandwidth_bytes'] = total_bytes
        print(f"✅ Tamamlandı. Buluta giden paket: {processed_count}")

    def generate_report(self):
        print("\n" + "="*70)
        print(f"{'FİNAL GÖRSELLEŞTİRME VE RAPOR':^70}")
        print("="*70)
        
        self.plot_advanced_charts()

    def plot_advanced_charts(self):
        """4'lü Gelişmiş Grafik Paneli"""
        print("\nGrafikler oluşturuluyor...")
        
        # Verileri hazırla
        scenarios = ['Edge (MQTT)', 'Cloud (MQTT)', 'Cloud (HTTP)']
        colors = ['#3498db', '#9b59b6', '#1abc9c'] 
        
        lat_data = [
            self.results['edge_mqtt']['latency'],
            self.results['cloud_mqtt']['latency'],
            self.results['cloud_http']['latency']
        ]
        
        bw_data = [
            self.results['edge_mqtt']['bandwidth_bytes'] / 1024,
            self.results['cloud_mqtt']['bandwidth_bytes'] / 1024,
            self.results['cloud_http']['bandwidth_bytes'] / 1024
        ]
        
        # HTTP'yi baz alarak tasarruf oranı hesapla
        base_bw = self.results['cloud_http']['bandwidth_bytes']
        savings = [
            (1 - (self.results['edge_mqtt']['bandwidth_bytes'] / base_bw)) * 100,
            (1 - (self.results['cloud_mqtt']['bandwidth_bytes'] / base_bw)) * 100,
            0.0
        ]

        # Çizim Alanı (2x2 Grid)
        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        plt.subplots_adjust(hspace=0.3, wspace=0.2)
        
        # 1. Gecikme Dağılımı (Boxplot) - EN ÖNEMLİ GRAFİK
        # Bu grafik min, max, medyan ve aykırı değerleri gösterir
        ax1 = axes[0, 0]
        bp = ax1.boxplot(lat_data, labels=scenarios, patch_artist=True, vert=True)
        
        # Renklendirme
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
            
        ax1.set_title('Gecikme Dağılımı ve Kararlılık (Boxplot)', fontweight='bold')
        ax1.set_ylabel('Gecikme (ms)')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # 2. Ortalama Gecikme (Bar Chart)
        ax2 = axes[0, 1]
        avg_lats = [np.mean(l) for l in lat_data]
        bars2 = ax2.bar(scenarios, avg_lats, color=colors, alpha=0.8)
        ax2.set_title('Ortalama Tepki Süresi', fontweight='bold')
        ax2.set_ylabel('Milisaniye (ms)')
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}ms', ha='center', va='bottom', fontweight='bold')

        # 3. Bant Genişliği Kullanımı (Bar Chart)
        ax3 = axes[1, 0]
        bars3 = ax3.bar(scenarios, bw_data, color=colors, alpha=0.8)
        ax3.set_title('Toplam Veri Kullanımı (KB)', fontweight='bold')
        ax3.set_ylabel('Kilobyte (KB)')
        for bar in bars3:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}KB', ha='center', va='bottom', fontweight='bold')

        # 4. Veri Tasarrufu Oranı (Bar Chart)
        ax4 = axes[1, 1]
        # Renkleri tasarruf için tersine çevir (Edge en iyi)
        save_colors = ['#27ae60', '#f39c12', '#95a5a6'] 
        bars4 = ax4.bar(scenarios, savings, color=save_colors, alpha=0.8)
        ax4.set_title('Cloud(HTTP) Senaryosuna Göre Veri Tasarrufu (%)', fontweight='bold')
        ax4.set_ylabel('Tasarruf Oranı (%)')
        ax4.set_ylim(0, 110)
        for bar in bars4:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'%{height:.1f}', ha='center', va='bottom', fontweight='bold')

        # Genel Başlık
        plt.suptitle(f' Kapsamlı Performans Analizi ({self.num_cycles} Veri Paketi)', 
                    fontsize=16, fontweight='bold')
        
        output_file = 'output/comprehensive_analysis.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Gelişmiş grafik paketi kaydedildi: {output_file}")
        plt.close()

if __name__ == "__main__":
    sim = ProtocolSimulation(num_cycles=2000, num_sensors=4)
    
    sim.run_edge_mqtt()
    sim.run_cloud_mqtt()
    sim.run_cloud_http()
    
    sim.generate_report()