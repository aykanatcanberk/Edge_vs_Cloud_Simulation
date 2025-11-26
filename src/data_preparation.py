"""
01
NASA Turbofan Engine Degradation Dataset Simülasyonu ve Hazırlama
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def create_directories():
    """Gerekli klasörleri oluştur"""
    os.makedirs('data', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    print("✓ Klasörler oluşturuldu/kontrol edildi")

def generate_turbofan_data():
    """NASA Turbofan veri setini simüle et"""
    print("\n" + "="*70)
    print("VERİ SETİ OLUŞTURMA")
    print("="*70)
    
    np.random.seed(42)
    
    # Parametreler
    num_engines = 4
    num_cycles = 2500         # ≈ 10.000 veri
    # num_cycles = 5000       # ≈ 20.000 veri 

    data_list = []
    
    print(f"\n{num_engines} motor için {num_cycles} döngülük veri oluşturuluyor...")
    
    for engine_id in range(1, num_engines + 1):
        for cycle in range(1, num_cycles + 1):
            degradation_factor = cycle / num_cycles
            
            temp_sensor1 = 520 + degradation_factor * 40 + np.random.normal(0, 2)
            temp_sensor2 = 640 + degradation_factor * 50 + np.random.normal(0, 3)
            pressure = 14.5 + degradation_factor * 2 + np.random.normal(0, 0.3)
            vibration = 0.02 + degradation_factor * 0.08 + np.random.normal(0, 0.005)
            rpm = 2300 + degradation_factor * 200 + np.random.normal(0, 20)
            
            health_indicator = 100 - (degradation_factor * 100)
            
            data_list.append({
                'engine_id': engine_id,
                'cycle': cycle,
                'sensor_temp1': round(temp_sensor1, 2),
                'sensor_temp2': round(temp_sensor2, 2),
                'sensor_pressure': round(pressure, 2),
                'sensor_vibration': round(vibration, 4),
                'sensor_rpm': round(rpm, 1),
                'health_indicator': round(health_indicator, 2)
            })
    
    df = pd.DataFrame(data_list)
    
    filepath = 'data/turbofan_sensor_data.csv'
    df.to_csv(filepath, index=False)
    print(f"✓ Veri seti kaydedildi: {filepath}")
    print(f"  Toplam kayıt: {len(df)}")
    print(f"  Motor sayısı: {num_engines}")
    print(f"  Her motor için döngü: {num_cycles}")
    
    return df

def analyze_data(df):
    """Veri setini analiz et"""
    print("\n" + "="*70)
    print("VERİ ANALİZİ")
    print("="*70)
    
    print("\n1. Veri Seti Boyutları:")
    print(f"   Satır x Sütun: {df.shape[0]} x {df.shape[1]}")
    
    print("\n2. Sensör Değer Aralıkları:")
    sensor_cols = [col for col in df.columns if 'sensor' in col or 'health' in col]
    for col in sensor_cols:
        print(f"   {col:25s}: {df[col].min():8.2f} - {df[col].max():8.2f}")
    
    print("\n3. İlk 5 Kayıt:")
    print(df.head().to_string())
    
    print("\n4. Son 5 Kayıt (Degradasyon etkisi):")
    print(df.tail().to_string())

def visualize_data(df):
    """Veri setini görselleştir"""
    print("GÖRSELLEŞTİRME")
    
    fig, axes = plt.subplots(3, 2, figsize=(16, 12))
    fig.suptitle('IoT Sensör Verilerinin Zamansal Analizi\n(NASA Turbofan Simülasyonu)', 
                 fontsize=16, fontweight='bold')
    
    sensor_cols = ['sensor_temp1', 'sensor_temp2', 'sensor_pressure', 
                   'sensor_vibration', 'sensor_rpm', 'health_indicator']
    titles = ['Sıcaklık Sensörü 1 (°F)', 'Sıcaklık Sensörü 2 (°F)', 
              'Basınç Sensörü (psi)', 'Titreşim Sensörü (g)', 
              'Devir Sayısı (RPM)', 'Sağlık Göstergesi (%)']
    
    for idx, (col, title) in enumerate(zip(sensor_cols, titles)):
        row = idx // 2
        col_idx = idx % 2
        ax = axes[row, col_idx]
        
        for engine in df['engine_id'].unique():
            engine_data = df[df['engine_id'] == engine]
            ax.plot(engine_data['cycle'], engine_data[col], 
                   label=f'Motor {engine}', alpha=0.7, linewidth=2)
        
        ax.set_xlabel('Çevrim (Zaman)', fontsize=10)
        ax.set_ylabel('Sensör Değeri', fontsize=10)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)
        
        if col == 'health_indicator':
            ax.axhline(y=30, color='red', linestyle='--', linewidth=2, alpha=0.7)
    
    plt.tight_layout()

    output_path = 'output/sensor_data_analysis.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Görselleştirme kaydedildi: {output_path}")
    
    plt.close()

def main():
    print("\n" + "="*70)
    print("ADIM 1: VERİ HAZIRLAMA VE KEŞFETME")
    print("="*70)
    
    create_directories()
    
    df = generate_turbofan_data()
    analyze_data(df)
    visualize_data(df)
    
if __name__ == "__main__":
    main()

