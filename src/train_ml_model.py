"""
02_train_ml_model.py
YÜKSEK PERFORMANSLI MODEL EĞİTİMİ (RANDOM FOREST)
Isolation Forest yerine Supervised Learning kullanılır.
"""

import pandas as pd
import numpy as np
import pickle
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

def train_high_performance_model():
    print("\n" + "="*70)
    print(" YÜKSEK PERFORMANSLI AI MODEL EĞİTİMİ (RANDOM FOREST)")
    print("="*70)
    
    os.makedirs('models', exist_ok=True)
    os.makedirs('output', exist_ok=True)
    
    # 1. Veri Yükleme
    try:
        df = pd.read_csv('data/turbofan_sensor_data.csv')
        print(f"✓ Veri seti yüklendi: {len(df)} kayıt")
    except FileNotFoundError:
        print("✗ Veri dosyası bulunamadı!")
        return

    # 2. Etiketleme (Labeling)
    # Sağlık < 30 ise ARIZA (1), değilse NORMAL (0)
    # Not: Random Forest için Arıza=1, Normal=0 standardını kullanacağız.
    ANOMALY_THRESHOLD = 30
    df['label'] = np.where(df['health_indicator'] <= ANOMALY_THRESHOLD, 1, 0)
    
    print(f"\nSınıf Dağılımı:")
    print(df['label'].value_counts().rename({0: 'Normal', 1: 'Arıza'}))

    # 3. Özellik Seçimi
    features = ['sensor_temp1', 'sensor_temp2', 'sensor_pressure', 'sensor_vibration', 'sensor_rpm']
    X = df[features]
    y = df['label']
    
    # 4. Eğitim/Test Bölmesi (%20 Test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 5. Model Eğitimi (Random Forest)
    print("\nModel eğitiliyor (Random Forest)...")
    # class_weight='balanced': Arıza verisi az olsa bile ona öncelik ver
    model = RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42)
    model.fit(X_train, y_train)
    
    # 6. Kaydetme (Scaler'a gerek yok, RF ölçekten bağımsız çalışır ama kod uyumu için boş geçebiliriz)
    with open('models/anomaly_detector.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    print("✓ Model 'models/anomaly_detector.pkl' olarak kaydedildi.")
    
    # 7. Performans Testi
    y_pred = model.predict(X_test)
    
    print("\n" + "-"*30)
    print(" MODEL PERFORMANS RAPORU")
    print("-" * 30)
    
    acc = accuracy_score(y_test, y_pred)
    print(f"GENEL DOĞRULUK (ACCURACY): %{acc*100:.2f}")
    print("\nDetaylı Rapor:")
    print(classification_report(y_test, y_pred, target_names=['Normal', 'Arıza']))
    
    # 8. Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Greens',
                xticklabels=['Tahmin: Normal', 'Tahmin: Arıza'],
                yticklabels=['Gerçek: Normal', 'Gerçek: Arıza'])
    plt.title(f'Random Forest Başarı Matrisi\nDoğruluk: %{acc*100:.1f}')
    plt.savefig('output/ai_model_accuracy_matrix.png')
    
    # 9. Özellik Önem Düzeyleri (Feature Importance)
    # Hangi sensör arızayı bulmada en etkili?
    feature_importance = pd.DataFrame({
        'Sensör': features,
        'Önem': model.feature_importances_
    }).sort_values('Önem', ascending=False)
    
    plt.figure(figsize=(10, 5))
    sns.barplot(x='Önem', y='Sensör', data=feature_importance, palette='viridis')
    plt.title('Hangi Sensör Arızayı Belirliyor? (Feature Importance)')
    plt.savefig('output/ai_feature_importance.png')
    
    print("\nÖzellik Önem Sıralaması:")
    print(feature_importance)
    print(f"\n✓ Grafikler 'output/' klasörüne kaydedildi.")
    plt.close('all')

if __name__ == "__main__":
    train_high_performance_model()