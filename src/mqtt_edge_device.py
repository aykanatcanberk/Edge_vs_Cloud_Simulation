"""
06_mqtt_edge_device.py
MQTT Protokolü ile Yüksek Performanslı AI Kenar Cihazı
(Random Forest Entegrasyonu)
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
    def __init__(self, device_id='edge_device_ai', broker='broker.hivemq.com', port=1883):
        self.device_id = device_id
        self.broker = broker
        self.port = port
        
        self.client = mqtt.Client(client_id=device_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.sensor_topic = "iot/sensors/+/data"
        self.cloud_topic = "iot/cloud/alerts"
        
        # AI Modeli
        self.ai_enabled = False
        self.model = None
        self.load_ai_models()
        
        self.sensor_history = {}
        self.window_size = 10
        
        # Sabit Eşikler (Yedek Güvenlik)
        self.thresholds = {
            'temperature_1': 560.0,
            'vibration': 0.12,
            'health': 30.0
        }
        
        self.metrics = {
            'total_received': 0,
            'ai_anomalies': 0,
            'rule_anomalies': 0,
            'cloud_messages_sent': 0,
            'local_decisions': 0,
            'processing_times': deque(maxlen=1000)
        }
        
        self.connected = False

    def load_ai_models(self):
        try:
            model_path = 'models/anomaly_detector.pkl'
            if os.path.exists(model_path):
                with open(model_path, 'rb') as f:
                    self.model = pickle.load(f)
                self.ai_enabled = True
                print(f"[{self.device_id}] ✓ GÜÇLÜ AI Modeli Yüklendi (Random Forest)")
            else:
                print(f"[{self.device_id}] ! AI Modeli bulunamadı.")
        except Exception as e:
            print(f"Model yükleme hatası: {e}")

    def connect(self):
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            return self.connected
        except:
            return False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            self.client.subscribe(self.sensor_topic)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            self.process_sensor_data(payload)
        except:
            pass

    def process_sensor_data(self, sensor_data):
        start_time = time.time()
        self.metrics['total_received'] += 1
        node_id = sensor_data.get('node_id')
        measurements = sensor_data.get('measurements', {})
        
        # --- 1. AI ANOMALİ TESPİTİ (RANDOM FOREST) ---
        ai_anomaly = False
        confidence = 0.0
        
        if self.ai_enabled:
            # Random Forest sıralamayı önemser, eğitimdeki sırayla aynı olmalı!
            features = [
                measurements.get('temperature_1', 0),
                measurements.get('temperature_2', 0),
                measurements.get('pressure', 0),
                measurements.get('vibration', 0),
                measurements.get('rpm', 0)
            ]
            
            try:
                # Random Forest için reshape gerekir: [[f1, f2, ...]]
                features_reshaped = np.array(features).reshape(1, -1)
                
                # Tahmin: 0=Normal, 1=Arıza
                prediction = self.model.predict(features_reshaped)[0]
                # Olasılık (Arıza olma ihtimali)
                probs = self.model.predict_proba(features_reshaped)
                confidence = probs[0][1]  # 1 sınıfının (Arıza) olasılığı
                
                if prediction == 1:
                    ai_anomaly = True
                    self.metrics['ai_anomalies'] += 1
                    # Arıza ihtimali yüksekse konsola bas
                    print(f"   🤖 [AI UYARISI] Node {node_id} -> Arıza Olasılığı: %{confidence*100:.1f}")
            except Exception as e:
                print(f"AI Hata: {e}")

        # --- 2. KURAL TABANLI KONTROL ---
        rule_anomalies = self.check_rules(measurements, sensor_data.get('health', 100))
        if rule_anomalies:
            self.metrics['rule_anomalies'] += len(rule_anomalies)
            print(f"    [KURAL] Node {node_id} -> Eşik Aşıldı")

        # --- 3. BİRLEŞTİRME ---
        final_anomalies = rule_anomalies
        if ai_anomaly:
            final_anomalies.append({
                'type': 'AI_DETECTED_FAILURE',
                'sensor': 'RandomForest',
                'value': float(confidence),
                'threshold': 0.5,
                'severity': 'CRITICAL' if confidence > 0.8 else 'WARNING'
            })

        # İşlem süresi
        self.metrics['processing_times'].append((time.time() - start_time) * 1000)

        # Eylem
        if final_anomalies:
            self.control_actuators(node_id, final_anomalies)
            self.send_to_cloud(sensor_data, final_anomalies)
            self.metrics['local_decisions'] += 1
        elif self.metrics['total_received'] % 20 == 0:
            self.send_summary_to_cloud(sensor_data)

    def check_rules(self, measurements, health):
        anomalies = []
        if measurements.get('temperature_1', 0) > self.thresholds['temperature_1']:
            anomalies.append({'type': 'THRESHOLD_TEMP', 'severity': 'WARNING'})
        if measurements.get('vibration', 0) > self.thresholds['vibration']:
            anomalies.append({'type': 'THRESHOLD_VIB', 'severity': 'CRITICAL'})
        return anomalies

    def control_actuators(self, node_id, anomalies):
        critical = any(a['severity'] == 'CRITICAL' for a in anomalies)
        action = 'EMERGENCY_STOP' if critical else 'MAINTENANCE_ALERT'
        msg = {'node_id': node_id, 'action': action, 'timestamp': datetime.now().isoformat()}
        self.client.publish(f"iot/actuators/{node_id}/command", json.dumps(msg))

    def send_to_cloud(self, sensor_data, anomalies):
        msg = {
            'device_id': self.device_id,
            'node_id': sensor_data.get('node_id'),
            'alert_type': 'ANOMALY',
            'anomalies': anomalies,
            'timestamp': datetime.now().isoformat()
        }
        self.client.publish(self.cloud_topic, json.dumps(msg))
        self.metrics['cloud_messages_sent'] += 1

    def send_summary_to_cloud(self, sensor_data):
        msg = {'device_id': self.device_id, 'alert_type': 'SUMMARY', 'node_id': sensor_data.get('node_id')}
        self.client.publish(self.cloud_topic, json.dumps(msg))
        self.metrics['cloud_messages_sent'] += 1

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def get_statistics(self):
        stats = self.metrics.copy()
        stats['anomalies_detected'] = stats['ai_anomalies'] + stats['rule_anomalies']
        if self.metrics['processing_times']:
            stats['avg_processing_time'] = statistics.mean(self.metrics['processing_times'])
        else:
            stats['avg_processing_time'] = 0
        return stats