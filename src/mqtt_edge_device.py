"""
06_mqtt_edge_device.py
MQTT Protokolü ile AI Destekli Kenar Bilişim Cihazı (GÜNCELLENDİ)
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
    """
    AI destekli MQTT kenar bilişim cihazı
    """
    
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
        self.scaler = None
        self.load_ai_models()
        
        self.sensor_history = {}
        self.window_size = 10
        
        self.thresholds = {
            'temperature_1': 560.0,
            'vibration': 0.12,
            'health': 30.0
        }
        
        # Metrikler (Detaylandırıldı)
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
            if os.path.exists('models/anomaly_detector.pkl'):
                with open('models/anomaly_detector.pkl', 'rb') as f:
                    self.model = pickle.load(f)
                with open('models/scaler.pkl', 'rb') as f:
                    self.scaler = pickle.load(f)
                self.ai_enabled = True
                print(f"[{self.device_id}] ✓ AI Modeli Yüklendi (Isolation Forest)")
            else:
                print(f"[{self.device_id}] ! AI Modeli bulunamadı.")
        except Exception as e:
            print(f"Model hatası: {e}")

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
        
        # --- 1. AI ANOMALİ TESPİTİ ---
        ai_anomaly = False
        ai_score = 0.0
        
        if self.ai_enabled:
            features = [
                measurements.get('temperature_1', 0),
                measurements.get('temperature_2', 0),
                measurements.get('pressure', 0),
                measurements.get('vibration', 0),
                measurements.get('rpm', 0)
            ]
            
            try:
                features_scaled = self.scaler.transform([features])
                prediction = self.model.predict(features_scaled)[0]
                ai_score = self.model.decision_function(features_scaled)[0]
                
                if prediction == -1:
                    ai_anomaly = True
                    self.metrics['ai_anomalies'] += 1
                    # GÖRSEL ÇIKTI EKLENDİ
                    print(f"   🤖 [AI TESPİTİ] Node {node_id} -> Anomali Skoru: {ai_score:.4f}")
            except:
                pass

        # --- 2. KURAL TABANLI TESPİT ---
        rule_anomalies = self.check_rules(measurements, sensor_data.get('health', 100))
        if rule_anomalies:
            self.metrics['rule_anomalies'] += len(rule_anomalies)
            # GÖRSEL ÇIKTI EKLENDİ
            print(f"   📏 [KURAL TESPİTİ] Node {node_id} -> {rule_anomalies[0]['type']}")

        # --- 3. KARAR BİRLEŞTİRME ---
        final_anomalies = rule_anomalies
        if ai_anomaly:
            final_anomalies.append({
                'type': 'AI_DETECTED_PATTERN',
                'sensor': 'isolation_forest',
                'value': float(ai_score),
                'threshold': 0,
                'severity': 'WARNING'
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
            anomalies.append({'type': 'THRESHOLD_TEMP', 'sensor': 'temp1', 'severity': 'WARNING', 'value': measurements['temperature_1']})
        if measurements.get('vibration', 0) > self.thresholds['vibration']:
            anomalies.append({'type': 'THRESHOLD_VIB', 'sensor': 'vibration', 'severity': 'CRITICAL', 'value': measurements['vibration']})
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