"""
06_mqtt_edge_device.py
Yapay Zeka Destekli Kenar Bilişim Cihazı (DÜZELTİLMİŞ VERSİYON)
Proje 10: Yerel İşleme ve Otonom Karar Mekanizması
"""

import json
import time
import statistics
import pickle
import numpy as np
import os
from collections import deque
from datetime import datetime
import paho.mqtt.client as mqtt

class MQTTEdgeDevice:
    def __init__(self, device_id='edge_pi_01', broker='broker.hivemq.com', port=1883):
        self.device_id = device_id
        self.broker = broker
        self.port = port
        
        self.client = mqtt.Client(client_id=device_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # 1. Konu Başlıkları
        self.sensor_topic = "iot/sensors/+/data"
        self.cloud_topic = "iot/cloud/alerts"
        self.actuator_topic = "iot/actuators/+/command"
        
        # 2. Yapay Zeka Modelini Yükle
        self.ai_enabled = False
        self.model = None
        self.load_ai_brain()
        
        # 3. Sabit Kurallar
        self.thresholds = {'temperature_1': 560.0, 'vibration': 0.12}
        
        # 4. Performans Metrikleri (Analiz dosyasıyla uyumlu isimler)
        self.metrics = {
            'total_received': 0,
            'ai_anomalies': 0,
            'cloud_messages_sent': 0,  
            'local_decisions': 0,     
            'processing_times': []
        }
        self.connected = False

    def load_ai_brain(self):
        """Eğitilmiş makine öğrenmesi modelini yükler"""
        try:
            # Model dosya yolu kontrolü 
            model_path = 'models/anomaly_detector.pkl'
            if not os.path.exists(model_path):
                model_path = '../models/anomaly_detector.pkl'
            
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    self.model = pickle.load(f)
                self.ai_enabled = True
                print(f"[{self.device_id}]  AI Modeli Yüklendi ve Aktif!")
            else:
                print(f"[{self.device_id}]  Model bulunamadı, sadece kural tabanlı çalışacak.")
        except Exception as e:
            print(f"Model yükleme hatası: {e}")

    def connect(self):
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            time.sleep(1)
            return True
        except:
            return False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            self.client.subscribe(self.sensor_topic)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            self.process_data_at_edge(payload)
        except:
            pass

    def process_data_at_edge(self, data):
        """
        PROJE ÇEKİRDEĞİ: Veri buluta gitmeden burada işleniyor.
        """
        start_time = time.time()
        self.metrics['total_received'] += 1
        node_id = data.get('node_id')
        measurements = data.get('measurements', {})
        
        anomalies = []
        
        # --- A. YAPAY ZEKA ANALİZİ ---
        if self.ai_enabled:
            try:
                features = np.array([
                    measurements.get('temperature_1', 0),
                    measurements.get('temperature_2', 0),
                    measurements.get('pressure', 0),
                    measurements.get('vibration', 0),
                    measurements.get('rpm', 0)
                ]).reshape(1, -1)
                
                prediction = self.model.predict(features)[0]
                
                if prediction == 1: # Arıza
                    probs = self.model.predict_proba(features)
                    confidence = probs[0][1]
                    
                    if confidence > 0.7:
                        self.metrics['ai_anomalies'] += 1
                        anomalies.append({
                            'type': 'AI_DETECTED_ANOMALY',
                            'confidence': f"%{confidence*100:.1f}",
                            'severity': 'CRITICAL'
                        })

                         #print(f"    [AI] Node {node_id} Anomali! (%{confidence*100:.0f})")
            except Exception as e:
                print(f"AI Hatası: {e}")

        # --- B. KURAL TABANLI ANALİZ ---
        if measurements.get('temperature_1', 0) > self.thresholds['temperature_1']:
            anomalies.append({'type': 'HIGH_TEMP', 'severity': 'WARNING'})

        # İşlem süresini kaydet
        proc_time = (time.time() - start_time) * 1000
        self.metrics['processing_times'].append(proc_time)

        # --- C. KARAR VE EYLEM ---
        if anomalies:
            self.trigger_actuator(node_id, anomalies)
            self.send_alert_to_cloud(data, anomalies)
        
        # Normal durumda veri azaltma (Heartbeat)
        elif self.metrics['total_received'] % 20 == 0:
            self.send_summary_to_cloud(data)

    def trigger_actuator(self, node_id, anomalies):
        """Yerel aktüatörleri tetikler"""
        action = "EMERGENCY_STOP" if any(a['severity'] == 'CRITICAL' for a in anomalies) else "ACTIVATE_FAN"
        
        msg = {
            'target_node': node_id,
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'source': 'EDGE_COMPUTING_UNIT'
        }
        self.client.publish(f"iot/actuators/{node_id}/command", json.dumps(msg))
        self.metrics['local_decisions'] += 1 

    def send_alert_to_cloud(self, raw_data, anomalies):
        """Sadece önemli veriyi gönder"""
        msg = {
            'type': 'ANOMALY_REPORT',
            'node_id': raw_data.get('node_id'),
            'anomalies': anomalies,
            'timestamp': datetime.now().isoformat()
        }
        self.client.publish(self.cloud_topic, json.dumps(msg))
        self.metrics['cloud_messages_sent'] += 1 

    def send_summary_to_cloud(self, raw_data):
        """Periyodik özet"""
        msg = {
            'type': 'STATUS_SUMMARY',
            'node_id': raw_data.get('node_id'),
            'avg_health': raw_data.get('health')
        }
        self.client.publish(self.cloud_topic, json.dumps(msg))
        self.metrics['cloud_messages_sent'] += 1 

    def get_statistics(self):
        stats = self.metrics.copy()
        if stats['processing_times']:
            stats['avg_proc_time'] = statistics.mean(stats['processing_times'])
        else:
            stats['avg_proc_time'] = 0
        return stats

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()