"""
02_train_ml_model.py
Kenar Cihaz için Anomali Tespit Modeli Eğitimi
Isolation Forest kullanarak normal/anormal durumları öğrenir
"""

import pandas as pd
import numpy as np
import pickle
import os
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

def train_anomaly_detector():
    print("YAPAY ZEKA MODELİ EĞİTİMİ (ISOLATION FOREST)")
    
    # Klasör kontrolü
    os.makedirs('models', exist_ok=True)
    
    # 1. Veri Yükleme
    try:
        df = pd.read_csv('data/turbofan_sensor_data.csv')
        print(f"✓ Veri seti yüklendi: {len(df)} kayıt")
    except FileNotFoundError:
        print("✗ Veri dosyası bulunamadı! Önce 01_data_generation.py çalıştırın.")
        return

    # 2. Özellik Seçimi (Feature Selection)
    # Anomali tespiti için kullanılacak sensörler
    features = ['sensor_temp1', 'sensor_temp2', 'sensor_pressure', 'sensor_vibration', 'sensor_rpm']
    X = df[features]
    
    # 3. Veri Ölçeklendirme (Standardization)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    print("✓ Veriler ölçeklendirildi (StandardScaler)")
    
    # 4. Model Eğitimi (Isolation Forest)
    # contamination='auto': Anomali oranını otomatik belirle
    print("\nModel eğitiliyor (bu işlem biraz sürebilir)...")
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    model.fit(X_scaled)
    
    # 5. Modeli ve Scaler'ı Kaydetme
    model_path = 'models/anomaly_detector.pkl'
    scaler_path = 'models/scaler.pkl'
    
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
        
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
        
    print(f"✓ Model kaydedildi: {model_path}")
    print(f"✓ Scaler kaydedildi: {scaler_path}")
    
    # 6. Test ve Görselleştirme
    # Tahmin yap (-1: Anomali, 1: Normal)
    predictions = model.predict(X_scaled)
    df['anomaly_score'] = model.decision_function(X_scaled)
    df['is_anomaly'] = predictions
    
    anomalies = df[df['is_anomaly'] == -1]
    print(f"\nEğitim Verisindeki Tespitler:")
    print(f"  Toplam Veri: {len(df)}")
    print(f"  Normal: {len(df[df['is_anomaly'] == 1])}")
    print(f"  Anomali: {len(anomalies)}")
    
    # Görselleştirme (Örnek: Titreşim vs Anomali Skoru)
    plt.figure(figsize=(12, 6))
    plt.scatter(df.index, df['sensor_vibration'], c=df['is_anomaly'], cmap='coolwarm', s=10, label='Veri')
    plt.title('AI Anomali Tespiti (Kırmızı: Anomali, Mavi: Normal)')
    plt.xlabel('Zaman (Veri Noktası)')
    plt.ylabel('Titreşim Sensörü')
    plt.colorbar(label='Durum (1: Normal, -1: Anomali)')
    
    viz_path = 'output/ai_model_visualization.png'
    plt.savefig(viz_path)
    print(f"✓ Görselleştirme kaydedildi: {viz_path}")

if __name__ == "__main__":
    train_anomaly_detector()