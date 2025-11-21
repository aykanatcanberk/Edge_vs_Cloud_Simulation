"""
02_train_ml_model.py
Kenar Cihaz için Anomali Tespit Modeli Eğitimi
Isolation Forest kullanarak tüm sensör verilerini analiz eder ve görselleştirir.
"""

import pandas as pd
import numpy as np
import pickle
import os
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

def train_anomaly_detector():
    print("\n" + "="*70)
    print("YAPAY ZEKA MODELİ EĞİTİMİ (ISOLATION FOREST)")
    print("="*70)
    
    # Klasör kontrolü
    os.makedirs('models', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    
    # 1. Veri Yükleme
    try:
        df = pd.read_csv('data/turbofan_sensor_data.csv')
        print(f"✓ Veri seti yüklendi: {len(df)} kayıt")
    except FileNotFoundError:
        print("✗ Veri dosyası bulunamadı! Önce 01_data_generation.py çalıştırın.")
        return

    # 2. Özellik Seçimi
    features = ['sensor_temp1', 'sensor_temp2', 'sensor_pressure', 'sensor_vibration', 'sensor_rpm']
    X = df[features]
    
    # 3. Veri Ölçeklendirme
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    print("✓ Veriler ölçeklendirildi (StandardScaler)")
    
    # 4. Model Eğitimi
    print("\nModel eğitiliyor (bu işlem biraz sürebilir)...")
    # contamination=0.05 -> Verinin %5'inin anomali olduğunu varsayıyoruz
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X_scaled)
    
    # 5. Kaydetme
    with open('models/anomaly_detector.pkl', 'wb') as f:
        pickle.dump(model, f)
    with open('models/scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)
        
    print("✓ Model ve Scaler 'models/' klasörüne kaydedildi.")
    
    # 6. Tahmin ve Skorlama
    df['is_anomaly'] = model.predict(X_scaled) # 1: Normal, -1: Anomali
    df['anomaly_score'] = model.decision_function(X_scaled) # Skor ne kadar düşükse o kadar anormal
    
    anomalies = df[df['is_anomaly'] == -1]
    print(f"\nSonuçlar:")
    print(f"  Normal Veri:   {len(df[df['is_anomaly'] == 1])}")
    print(f"  Anomali Veri:  {len(anomalies)}")
    
    # 7. GELİŞMİŞ GÖRSELLEŞTİRME (TÜM SENSÖRLER)
    print("\nGrafikler oluşturuluyor...")
    
    # Grafiği ayarla: 3 satır, 2 sütun (Toplam 6 grafik)
    fig, axes = plt.subplots(3, 2, figsize=(18, 12))
    fig.suptitle('Yapay Zeka Anomali Analizi: Tüm Sensörler\n(Mavi: Normal, Kırmızı: Anomali)', fontsize=16, fontweight='bold')
    
    # Çizilecek veriler
    plot_cols = features + ['anomaly_score']
    titles = [
        'Sıcaklık 1 (Temp1)', 'Sıcaklık 2 (Temp2)', 
        'Basınç (Pressure)', 'Titreşim (Vibration)', 
        'Devir (RPM)', 'Model Anomali Skoru'
    ]
    
    # Düzleştirilmiş eksen listesi (döngü için)
    axes_flat = axes.flatten()
    
    for i, col in enumerate(plot_cols):
        ax = axes_flat[i]
        
        # Normal verileri çiz (Mavi, küçük nokta, şeffaf)
        normal = df[df['is_anomaly'] == 1]
        ax.scatter(normal.index, normal[col], c='blue', s=2, alpha=0.3, label='Normal')
        
        # Anomalileri çiz (Kırmızı, büyük nokta, belirgin)
        anomaly = df[df['is_anomaly'] == -1]
        ax.scatter(anomaly.index, anomaly[col], c='red', s=10, alpha=0.8, label='Anomali')
        
        ax.set_title(titles[i], fontweight='bold')
        ax.set_xlabel('Zaman (Veri İndeksi)')
        ax.set_ylabel('Değer')
        ax.grid(True, alpha=0.3)
        
        if i == 0:
            ax.legend(loc='upper left')

    plt.tight_layout()
    
    viz_path = 'output/ai_all_sensors_analysis.png'
    plt.savefig(viz_path, dpi=300)
    print(f"✓ Detaylı görselleştirme kaydedildi: {viz_path}")
    plt.close()

if __name__ == "__main__":
    train_anomaly_detector()