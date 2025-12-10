

# """
# ROBUST YEA Fraud Detection - Kafka Consumer (KAFKA-ONLY MODE)
# ✅ NO CUSTOMER API DEPENDENCY
# ✅ Uses ONLY Kafka message fields
# ✅ Enhanced features: device_id, phone_number, debit_account, status
# ✅ Continues fraud detection even with minimal data
# """

# import json
# import logging
# import requests
# from datetime import datetime
# from kafka import KafkaConsumer
# from typing import Dict, Optional
# from collections import defaultdict
# import sqlite3

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)


# class KafkaOnlyFraudDetectionConsumer:
#     """Kafka-only consumer: no external dependencies"""

#     # Real channelId values from Kafka (for one-hot encoding if needed)
#     CHANNEL_IDS = [
#         '2E08D0409BE26BA3E064020820E70A68',
#         '2E08D0409BE76BA3E064020820E70A68',
#         '2E08D0409BE16BA3E064020820E70A68',
#         '2E08D0409BE46BA3E064020820E70A68'
#     ]

#     # Transaction types that trigger fraud check
#     PAYMENT_TYPES = [
#         'PAY_TO_COOPTILL',
#         'POST_DIRECT_PAYMENTS',
#         'PERSONAL_ELOAN',
#         'COOP_REMIT_POST',
#         'INSTITUTIONAL_PAYMENT_REQUEST'
#     ]

#     # Default fallback values (for missing optional fields)
#     DEFAULTS = {
#         'date_of_birth': '1990-01-01',
#         'gender': 'F',
#         'relationship_opening_date': '2023-01-01',
#         'risk_rating': 'LOW',
#         'customer_status': 'ACTVE'
#     }

#     def __init__(
#         self,
#         kafka_bootstrap_servers: str,
#         kafka_topic: str,
#         kafka_group_id: str,
#         fraud_api_url: str
#     ):
#         self.kafka_bootstrap_servers = kafka_bootstrap_servers
#         self.kafka_topic = kafka_topic
#         self.kafka_group_id = kafka_group_id
#         self.fraud_api_url = fraud_api_url
#         self.consumer = None

#         # Historical tracking for velocity & device
#         self.device_history = defaultdict(set)      # customer_id -> {device_id}
#         self.account_history = defaultdict(list)    # debit_account -> [timestamps]
#         self.failed_attempts = defaultdict(list)    # customer_id -> [timestamps]

#         # Stats
#         self.stats = {
#             'total_processed': 0,
#             'fraud_detected': 0,
#             'blocked': 0,
#             'reviewed': 0,
#             'approved': 0,
#             'errors': 0,
#             'api_calls': 0,
#             'device_switches': 0,
#             'velocity_alerts': 0,
#             'failed_attempt_alerts': 0
#         }

#     def initialize_consumer(self) -> bool:
#         try:
#             self.consumer = KafkaConsumer(
#                 self.kafka_topic,
#                 bootstrap_servers=self.kafka_bootstrap_servers,
#                 group_id=self.kafka_group_id,
#                 auto_offset_reset='latest',
#                 enable_auto_commit=True,
#                 value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#             )
#             logger.info(f"✅ Kafka consumer initialized: {self.kafka_topic}")
#             return True
#         except Exception as e:
#             logger.error(f"❌ Failed to initialize Kafka: {str(e)}")
#             return False

#     def should_check_fraud(self, transaction: Dict) -> bool:
#         amount = float(transaction.get('transaction_amount', 0))
#         if amount == 0:
#             return False
#         txn_type = transaction.get('transaction_type', '')
#         return txn_type in self.PAYMENT_TYPES

#     def extract_enhanced_features(self, txn: Dict) -> Dict:
#         """Extract all available features from Kafka message with fallbacks"""
#         from datetime import datetime, timedelta
#         now = datetime.now()

#         customer_id = txn.get('customer_id', 'N/A')
#         device_id = txn.get('device_id', 'UNKNOWN')
#         debit_account = txn.get('debit_account', '')
#         status = txn.get('status', 200)
#         phone_number = txn.get('phone_number', '')

#         # === Device Tracking ===
#         known_devices = self.device_history.get(customer_id, set())
#         is_new_device = 1 if (device_id not in known_devices and device_id != 'UNKNOWN') else 0
#         if is_new_device:
#             self.stats['device_switches'] += 1
#             logger.warning(f"🔄 NEW DEVICE: Customer {customer_id}, Device: {device_id}")
#         if device_id != 'UNKNOWN':
#             self.device_history[customer_id].add(device_id)

#         # === Velocity (transactions per day per account) ===
#         transactions_today = 0
#         if debit_account:
#             today_txns = [
#                 t for t in self.account_history[debit_account]
#                 if (now - t).days == 0
#             ]
#             transactions_today = len(today_txns)
#             self.account_history[debit_account].append(now)
#             if transactions_today >= 5:
#                 self.stats['velocity_alerts'] += 1
#                 logger.warning(f"⚡ VELOCITY ALERT: Account {debit_account}, {transactions_today} txns today")

#         # === Failed Attempts (last hour) ===
#         if status != 200:
#             self.failed_attempts[customer_id].append(now)
#         recent_failures = [
#             t for t in self.failed_attempts.get(customer_id, [])
#             if (now - t).seconds < 3600
#         ]
#         failed_attempts_last_hour = len(recent_failures)
#         if failed_attempts_last_hour >= 3:
#             self.stats['failed_attempt_alerts'] += 1
#             logger.critical(f"🚨 MULTIPLE FAILURES: Customer {customer_id}, {failed_attempts_last_hour} attempts")

#         return {
#             'device_id': device_id,
#             'is_new_device': is_new_device,
#             'phone_number': phone_number,
#             'debit_account': debit_account,
#             'transactions_today': transactions_today,
#             'failed_attempts_last_hour': failed_attempts_last_hour,
#             'status': status
#         }

#     def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
#         try:
#             self.stats['api_calls'] += 1
#             response = requests.post(
#                 self.fraud_api_url,
#                 json=transaction_data,
#                 timeout=5,
#                 headers={'Content-Type': 'application/json'}
#             )
#             return response.json() if response.status_code == 200 else None
#         except Exception as e:
#             logger.error(f"Error calling fraud API: {str(e)}")
#             return None

#     def handle_fraud_result(self, transaction: Dict, fraud_result: Dict, enhanced: Dict):
#         recommendation = fraud_result.get('recommendation', 'APPROVE')
#         risk_level = fraud_result.get('risk_level', 'UNKNOWN')
#         fraud_score = fraud_result.get('fraud_score', 0)
#         txn_id = transaction.get('reference') or transaction.get('Id', 'N/A')
#         amount = transaction.get('transaction_amount', 0)

#         # Build extra info string
#         extra = []
#         if enhanced.get('is_new_device'):
#             extra.append("NEW_DEVICE")
#         if enhanced.get('transactions_today', 0) >= 5:
#             extra.append(f"HIGH_VELOCITY({enhanced['transactions_today']})")
#         if enhanced.get('failed_attempts_last_hour', 0) > 0:
#             extra.append(f"FAILED_ATTEMPTS({enhanced['failed_attempts_last_hour']})")
#         extra_str = f" | {', '.join(extra)}" if extra else ""

#         if recommendation == 'BLOCK':
#             self.stats['blocked'] += 1
#             logger.critical(f"🚨 BLOCK: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")
#         elif recommendation == 'REVIEW':
#             self.stats['reviewed'] += 1
#             logger.warning(f"⚠️  REVIEW: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")
#         else:
#             self.stats['approved'] += 1
#             logger.info(f"✅ APPROVE: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")

#         self.log_to_dashboard(transaction, fraud_result)

#     def process_transaction(self, kafka_message: Dict):
#         try:
#             transaction_id = kafka_message.get('reference') or kafka_message.get('Id', 'N/A')
#             customer_id = kafka_message.get('customer_id', 'N/A')
#             amount = kafka_message.get('transaction_amount', 0)
#             txn_type = kafka_message.get('transaction_type', 'UNKNOWN')

#             logger.info(f"📥 Transaction: {transaction_id}, Customer: {customer_id}, Amount: {amount}, Type: {txn_type}")

#             if not self.should_check_fraud(kafka_message):
#                 logger.info(f"⏭️ Skipping non-payment: {txn_type}")
#                 return

#             # Extract enhanced features from Kafka
#             enhanced = self.extract_enhanced_features(kafka_message)

#             # Build fraud request using ONLY Kafka data + defaults
#             fraud_request = {
#                 # Required by API (with defaults)
#                 'transaction_id': transaction_id,
#                 'customer_id': customer_id,
#                 'date_of_birth': self.DEFAULTS['date_of_birth'],
#                 'gender': self.DEFAULTS['gender'],
#                 'transaction_amount': str(amount),
#                 'transaction_date': kafka_message.get('transaction_date', datetime.now().isoformat()),
#                 'transaction_type': txn_type,
#                 'channelId': kafka_message.get('channelId', self.CHANNEL_IDS[0]),
#                 'relationship_opening_date': self.DEFAULTS['relationship_opening_date'],
#                 'risk_rating': self.DEFAULTS['risk_rating'],
#                 'customer_status': self.DEFAULTS['customer_status'],

#                 # Enhanced Kafka features (for rule-based logic in API)
#                 'is_new_device': enhanced['is_new_device'],
#                 'transactions_today': enhanced['transactions_today'],
#                 'failed_attempts_last_hour': enhanced['failed_attempts_last_hour'],
#                 'phone_mismatch': 0,  # SIM swap requires Customer API → disabled in Kafka-only
#             }

#             fraud_result = self.call_fraud_api(fraud_request)
#             if not fraud_result:
#                 logger.error(f"❌ Fraud API failed for {transaction_id}")
#                 self.stats['errors'] += 1
#                 return

#             self.stats['total_processed'] += 1
#             if fraud_result.get('fraud_prediction') == 1:
#                 self.stats['fraud_detected'] += 1

#             self.handle_fraud_result(kafka_message, fraud_result, enhanced)

#         except Exception as e:
#             logger.error(f"❌ Error processing transaction: {str(e)}", exc_info=True)
#             self.stats['errors'] += 1

#     def log_to_dashboard(self, transaction: Dict, fraud_result: Dict):
#         try:
#             conn = sqlite3.connect('fraud_events.db')
#             c = conn.cursor()
#             c.execute("""CREATE TABLE IF NOT EXISTS fraud_events (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 timestamp TEXT,
#                 transaction_id TEXT,
#                 customer_id TEXT,
#                 amount REAL,
#                 transaction_type TEXT,
#                 fraud_prediction INTEGER,
#                 risk_level TEXT,
#                 recommendation TEXT,
#                 fraud_score REAL,
#                 processing_time_ms REAL
#             )""")
#             c.execute("""INSERT INTO fraud_events 
#                 (timestamp, transaction_id, customer_id, amount, transaction_type,
#                 fraud_prediction, risk_level, recommendation, fraud_score, processing_time_ms)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
#                 (
#                     datetime.now().isoformat(),
#                     transaction.get('reference', 'N/A'),
#                     transaction.get('customer_id'),
#                     float(transaction.get('transaction_amount', 0)),
#                     transaction.get('transaction_type', 'UNKNOWN'),
#                     fraud_result.get('fraud_prediction'),
#                     fraud_result.get('risk_level'),
#                     fraud_result.get('recommendation'),
#                     fraud_result.get('fraud_score'),
#                     fraud_result.get('processing_time_ms', 0)
#                 ))
#             conn.commit()
#             conn.close()
#         except Exception as e:
#             logger.debug(f"Dashboard logging error: {e}")

#     def print_stats(self):
#         total = self.stats['total_processed']
#         fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
#         logger.info(f"\n{'='*70}")
#         logger.info(f"📊 KAFKA-ONLY STATISTICS")
#         logger.info(f"{'='*70}")
#         logger.info(f"Total Processed:  {total}")
#         logger.info(f"Fraud Detected:   {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
#         logger.info(f"  - Blocked:      {self.stats['blocked']}")
#         logger.info(f"  - Reviewed:     {self.stats['reviewed']}")
#         logger.info(f"  - Approved:     {self.stats['approved']}")
#         logger.info(f"\nEnhanced Detections:")
#         logger.info(f"  - Device Switches:     {self.stats['device_switches']}")
#         logger.info(f"  - Velocity Alerts:     {self.stats['velocity_alerts']}")
#         logger.info(f"  - Failed Attempts:     {self.stats['failed_attempt_alerts']}")
#         logger.info(f"\nErrors:           {self.stats['errors']}")
#         logger.info(f"API Calls:        {self.stats['api_calls']}")
#         logger.info(f"{'='*70}\n")

#     def start(self):
#         if not self.initialize_consumer():
#             logger.error("Failed to initialize consumer")
#             return

#         logger.info(f"\n{'='*70}")
#         logger.info(f"🚀 KAFKA-ONLY FRAUD DETECTION - STARTED")
#         logger.info(f"{'='*70}")
#         logger.info(f"Kafka: {self.kafka_bootstrap_servers}")
#         logger.info(f"Topic: {self.kafka_topic}")
#         logger.info(f"Mode:  KAFKA-ONLY (NO Customer API)")
#         logger.info(f"{'='*70}\n")

#         try:
#             for message in self.consumer:
#                 try:
#                     self.process_transaction(message.value)
#                     if self.stats['total_processed'] % 10 == 0 and self.stats['total_processed'] > 0:
#                         self.print_stats()
#                 except Exception as e:
#                     logger.error(f"Message handler error: {str(e)}", exc_info=True)
#                     continue
#         except KeyboardInterrupt:
#             logger.info("\n⚠️  Shutting down...")
#         finally:
#             if self.consumer:
#                 self.consumer.close()
#             self.print_stats()
#             logger.info("✅ Consumer stopped")


# def main():
#     KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76:9092"
#     KAFKA_TOPIC = "YEA_Transaction_Monitoring"
#     KAFKA_GROUP_ID = "fraud_detection_consumer_kafka_only"
#     FRAUD_API_URL = "http://localhost:8000/fraud/predict"

#     print(f"\n{'='*70}")
#     print("🛡️  KAFKA-ONLY FRAUD DETECTION CONSUMER")
#     print(f"{'='*70}")
#     print(f"\n⚙️  Configuration:")
#     print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
#     print(f"   Topic: {KAFKA_TOPIC}")
#     print(f"   Fraud API: {FRAUD_API_URL}")
#     print(f"\n✅ Mode: Kafka-Only (No Customer API)")
#     print(f"✅ Enhanced Features: Device, Velocity, Failed Attempts")
#     print(f"\n{'='*70}")

#     response = input("\n✅ Start Kafka-only consumer? (y/n): ")
#     if response.lower() != 'y':
#         print("Cancelled.")
#         return

#     consumer = KafkaOnlyFraudDetectionConsumer(
#         kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         kafka_topic=KAFKA_TOPIC,
#         kafka_group_id=KAFKA_GROUP_ID,
#         fraud_api_url=FRAUD_API_URL
#     )
#     consumer.start()


# if __name__ == "__main__":
#     main()


"""
Kafka Consumer for Kafka-Only Model (37 features)
 NO CUSTOMER API DEPENDENCY
 Extracts all 37 features from Kafka messages
 Enhanced tracking: device, velocity, failed attempts
"""

import json
import logging
import requests
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from typing import Dict, Optional
from collections import defaultdict
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaOnlyFraudConsumer:
    """Kafka-only consumer - no Customer API dependency"""
    
    # All 20 payment types
    PAYMENT_TYPES = [
        'PAY_TO_COOPTILL',
        'POST_DIRECT_PAYMENTS',
        'PERSONAL_ELOAN',
        'COOP_REMIT_POST',
        'INSTITUTIONAL_PAYMENT_REQUEST',
        'IFT_OWN',
        'IFT_OTHER',
        'STANDING_ORDER_POSTING',
        'BILL_PAYMENT',
        'RTGS_POST_PAYMENTS',
        'SWIFT_TRANSACTION',
        'MPESA_B2C',
        'MPESA_C2B',
        'MPESA_PAY_TO_TILL',
        'SEND_CASH_BY_CODE_ATM',
        'SEND_CASH_BY_CODE_AGENT',
        'CARDLESS_WITHDRAWAL_ATM',
        'CARDLESS_WITHDRAWAL_AGENT',
        'FLOAT_PURCHASE',
        'SAFARICOM_AIRTIME_PURCHASE'
    ]
    
    # All 6 channels
    CHANNEL_IDS = [
        '2E08D0409BE26BA3E064020820E70A68',
        '2E08D0409BE76BA3E064020820E70A68',
        '2E08D0409BE16BA3E064020820E70A68',
        '2E08D0409BE46BA3E064020820E70A68',
        '2E08D0409BE66BA3E064020820E70A68',
        '2E08D0409BE86BA3E064020820E70A68'
    ]
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        fraud_api_url: str
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.fraud_api_url = fraud_api_url
        self.consumer = None
        
        # Enhanced tracking
        self.device_history = defaultdict(set)  # customer_id -> {device_ids}
        self.account_history = defaultdict(list)  # debit_account -> [timestamps]
        self.failed_attempts = defaultdict(list)  # customer_id -> [timestamps]
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'fraud_detected': 0,
            'blocked': 0,
            'reviewed': 0,
            'approved': 0,
            'skipped_non_payment': 0,
            'errors': 0,
            'device_switches': 0,
            'velocity_alerts': 0,
            'failed_attempt_alerts': 0
        }
    
    def initialize_consumer(self) -> bool:
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.kafka_group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f" Connected to Kafka: {self.kafka_topic}")
            return True
        except Exception as e:
            logger.error(f" Failed to connect to Kafka: {str(e)}")
            return False
    
    def should_check_fraud(self, transaction: Dict) -> bool:
        """Check if transaction should go through fraud detection"""
        # Must have amount
        amount = float(transaction.get('transaction_amount', 0))
        if amount == 0:
            return False
        
        # Must be payment type
        txn_type = transaction.get('transaction_type', '')
        return txn_type in self.PAYMENT_TYPES
    
    def extract_enhanced_features(self, kafka_msg: Dict) -> Dict:
        """Extract enhanced features from Kafka message"""
        now = datetime.now()
        
        customer_id = kafka_msg.get('customer_id', 'N/A')
        device_id = kafka_msg.get('device_id', 'UNKNOWN')
        debit_account = kafka_msg.get('debit_account', '')
        status_val = kafka_msg.get('status', 1)
        phone_number = kafka_msg.get('phone_number', '')
        
        # === DEVICE TRACKING ===
        known_devices = self.device_history.get(customer_id, set())
        is_new_device = 0
        
        if device_id and device_id != 'UNKNOWN':
            if device_id not in known_devices:
                is_new_device = 1
                self.stats['device_switches'] += 1
                logger.warning(f" NEW DEVICE: Customer {customer_id}, Device: {device_id}")
            self.device_history[customer_id].add(device_id)
        
        # === VELOCITY MONITORING ===
        transactions_today = 0
        if debit_account:
            # Count transactions today
            today_txns = [
                t for t in self.account_history[debit_account]
                if (now - t).days == 0
            ]
            transactions_today = len(today_txns)
            self.account_history[debit_account].append(now)
            
            if transactions_today >= 5:
                self.stats['velocity_alerts'] += 1
                logger.warning(f" VELOCITY: Account {debit_account}, {transactions_today} txns today")
        
        # === FAILED ATTEMPTS ===
        failed_attempts_last_hour = 0
        try:
            status_int = int(status_val)
            if status_int == 0:  # Failed transaction
                self.failed_attempts[customer_id].append(now)
        except:
            pass
        
        # Count recent failures (last hour)
        recent_failures = [
            t for t in self.failed_attempts.get(customer_id, [])
            if (now - t).seconds < 3600
        ]
        failed_attempts_last_hour = len(recent_failures)
        
        if failed_attempts_last_hour >= 3:
            self.stats['failed_attempt_alerts'] += 1
            logger.critical(f" FAILED ATTEMPTS: Customer {customer_id}, {failed_attempts_last_hour} in last hour")
        
        return {
            'device_id': device_id,
            'is_new_device': is_new_device,
            'transactions_today': transactions_today,
            'failed_attempts_last_hour': failed_attempts_last_hour,
            'phone_mismatch': 0  # Not available without Customer API
        }
    
    def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
        """Call fraud detection API"""
        try:
            response = requests.post(
                self.fraud_api_url,
                json=transaction_data,
                timeout=5,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Fraud API returned {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling fraud API: {str(e)}")
            return None
    
    def handle_fraud_result(self, transaction: Dict, fraud_result: Dict, enhanced: Dict):
        """Handle and log fraud detection result"""
        recommendation = fraud_result.get('recommendation', 'APPROVE')
        risk_level = fraud_result.get('risk_level', 'UNKNOWN')
        fraud_score = fraud_result.get('fraud_score', 0)
        triggered_rules = fraud_result.get('triggered_rules', [])
        
        txn_id = transaction.get('reference') or transaction.get('id', 'N/A')
        amount = transaction.get('transaction_amount', 0)
        
        # Build context string
        context = []
        if enhanced.get('is_new_device'):
            context.append("NEW_DEVICE")
        if enhanced.get('transactions_today', 0) >= 5:
            context.append(f"VELOCITY({enhanced['transactions_today']})")
        if enhanced.get('failed_attempts_last_hour', 0) > 0:
            context.append(f"FAILED({enhanced['failed_attempts_last_hour']})")
        
        context_str = f" | {', '.join(context)}" if context else ""
        rules_str = f" | Rules: {', '.join(triggered_rules)}" if triggered_rules else ""
        
        # Log based on recommendation
        if recommendation == 'BLOCK':
            self.stats['blocked'] += 1
            logger.critical(
                f" BLOCK: TXN={txn_id}, Amount={amount}, "
                f"Risk={risk_level}, Score={fraud_score:.2f}"
                f"{context_str}{rules_str}"
            )
        elif recommendation == 'REVIEW':
            self.stats['reviewed'] += 1
            logger.warning(
                f"  REVIEW: TXN={txn_id}, Amount={amount}, "
                f"Risk={risk_level}, Score={fraud_score:.2f}"
                f"{context_str}{rules_str}"
            )
        else:
            self.stats['approved'] += 1
            logger.info(
                f" APPROVE: TXN={txn_id}, Amount={amount}, "
                f"Risk={risk_level}, Score={fraud_score:.2f}"
                f"{context_str}{rules_str}"
            )
        
        # Log to database
        self.log_to_dashboard(transaction, fraud_result)
    
    def process_transaction(self, kafka_msg: Dict):
        """Process a single transaction"""
        try:
            txn_id = kafka_msg.get('reference') or kafka_msg.get('id', 'N/A')
            customer_id = kafka_msg.get('customer_id', 'N/A')
            amount = kafka_msg.get('transaction_amount', 0)
            txn_type = kafka_msg.get('transaction_type', 'UNKNOWN')
            
            logger.info(
                f" Transaction: {txn_id}, Customer: {customer_id}, "
                f"Amount: {amount}, Type: {txn_type}"
            )
            
            # Check if should process
            if not self.should_check_fraud(kafka_msg):
                self.stats['skipped_non_payment'] += 1
                logger.debug(f"  Skipping non-payment: {txn_type}")
                return
            
            # Extract enhanced features
            enhanced = self.extract_enhanced_features(kafka_msg)
            
            # Build fraud request with all Kafka fields
            fraud_request = {
                # Basic
                'transaction_id': txn_id,
                'customer_id': customer_id,
                'transaction_amount': str(amount),
                'transaction_date': kafka_msg.get('transaction_date', datetime.now().isoformat()),
                'transaction_type': txn_type,
                
                # Kafka fields
                'channelId': kafka_msg.get('channelId', self.CHANNEL_IDS[0]),
                'channel': kafka_msg.get('channel', '12'),
                'device_id': kafka_msg.get('device_id', 'UNKNOWN'),
                'phone_number': kafka_msg.get('phone_number', ''),
                'status': kafka_msg.get('status', 1),
                'transaction_currency': kafka_msg.get('transaction_currency', 'KES'),
                'credit_account_id': kafka_msg.get('credit_account_id', ''),
                
                # Enhanced features
                'is_new_device': enhanced['is_new_device'],
                'transactions_today': enhanced['transactions_today'],
                'failed_attempts_last_hour': enhanced['failed_attempts_last_hour'],
                'phone_mismatch': enhanced['phone_mismatch']
            }
            
            # Call fraud API
            fraud_result = self.call_fraud_api(fraud_request)
            
            if not fraud_result:
                logger.error(f" Fraud API failed for {txn_id}")
                self.stats['errors'] += 1
                return
            
            # Update stats
            self.stats['total_processed'] += 1
            if fraud_result.get('fraud_prediction') == 1:
                self.stats['fraud_detected'] += 1
            
            # Handle result
            self.handle_fraud_result(kafka_msg, fraud_result, enhanced)
            
        except Exception as e:
            logger.error(f" Error processing transaction: {str(e)}", exc_info=True)
            self.stats['errors'] += 1
    
    def log_to_dashboard(self, transaction: Dict, fraud_result: Dict):
        """Log to dashboard database"""
        try:
            conn = sqlite3.connect('fraud_events.db')
            c = conn.cursor()
            
            c.execute("""CREATE TABLE IF NOT EXISTS fraud_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                transaction_id TEXT,
                customer_id TEXT,
                amount REAL,
                transaction_type TEXT,
                fraud_prediction INTEGER,
                risk_level TEXT,
                recommendation TEXT,
                fraud_score REAL,
                processing_time_ms REAL
            )""")
            
            c.execute("""INSERT INTO fraud_events 
                (timestamp, transaction_id, customer_id, amount, transaction_type,
                fraud_prediction, risk_level, recommendation, fraud_score, processing_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().isoformat(),
                    transaction.get('reference', 'N/A'),
                    transaction.get('customer_id'),
                    float(transaction.get('transaction_amount', 0)),
                    transaction.get('transaction_type', 'UNKNOWN'),
                    fraud_result.get('fraud_prediction'),
                    fraud_result.get('risk_level'),
                    fraud_result.get('recommendation'),
                    fraud_result.get('fraud_score'),
                    fraud_result.get('processing_time_ms', 0)
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Dashboard logging error: {e}")
    
    def print_stats(self):
        """Print statistics"""
        total = self.stats['total_processed']
        fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
        
        logger.info(f"\n{'='*70}")
        logger.info(f" KAFKA-ONLY FRAUD DETECTION STATISTICS")
        logger.info(f"{'='*70}")
        logger.info(f"Total Processed:     {total}")
        logger.info(f"Fraud Detected:      {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
        logger.info(f"  - Blocked:         {self.stats['blocked']}")
        logger.info(f"  - Reviewed:        {self.stats['reviewed']}")
        logger.info(f"  - Approved:        {self.stats['approved']}")
        logger.info(f"\nEnhanced Detections:")
        logger.info(f"  - Device Switches: {self.stats['device_switches']}")
        logger.info(f"  - Velocity Alerts: {self.stats['velocity_alerts']}")
        logger.info(f"  - Failed Attempts: {self.stats['failed_attempt_alerts']}")
        logger.info(f"\nSkipped:             {self.stats['skipped_non_payment']}")
        logger.info(f"Errors:              {self.stats['errors']}")
        logger.info(f"{'='*70}\n")
    
    def start(self):
        """Start consuming messages"""
        if not self.initialize_consumer():
            logger.error("Failed to initialize consumer")
            return
        
        logger.info(f"\n{'='*70}")
        logger.info(f" KAFKA-ONLY FRAUD DETECTION - STARTED")
        logger.info(f"{'='*70}")
        logger.info(f"Kafka:    {self.kafka_bootstrap_servers}")
        logger.info(f"Topic:    {self.kafka_topic}")
        logger.info(f"Group:    {self.kafka_group_id}")
        logger.info(f"Mode:     KAFKA-ONLY (37 features, no Customer API)")
        logger.info(f"\nFeatures:")
        logger.info(f"   Device tracking")
        logger.info(f"   Velocity monitoring")
        logger.info(f"   Failed attempt tracking")
        logger.info(f"   All 20 payment types")
        logger.info(f"   All 6 channels")
        logger.info(f"{'='*70}\n")
        
        try:
            for message in self.consumer:
                try:
                    self.process_transaction(message.value)
                    
                    # Print stats every 10 transactions
                    if self.stats['total_processed'] % 10 == 0 and self.stats['total_processed'] > 0:
                        self.print_stats()
                        
                except Exception as e:
                    logger.error(f"Message handler error: {str(e)}", exc_info=True)
                    continue
                    
        except KeyboardInterrupt:
            logger.info("\n  Shutting down...")
        finally:
            if self.consumer:
                self.consumer.close()
            self.print_stats()
            logger.info(" Consumer stopped")


def main():
    """Main entry point"""
    
    KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76:9092"
    KAFKA_TOPIC = "YEA_Transaction_Monitoring"
    KAFKA_GROUP_ID = "fraud_detection_kafka_only"
    FRAUD_API_URL = "http://localhost:8000/fraud/predict/"
    
    print("\n" + "="*70)
    print("  KAFKA-ONLY FRAUD DETECTION CONSUMER")
    print("="*70)
    print(f"\n⚙️  Configuration:")
    print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Fraud API: {FRAUD_API_URL}")
    print(f"\n Mode: Kafka-Only (37 features)")
    print(f"   - NO Customer API dependency")
    print(f"   - Device tracking: ENABLED")
    print(f"   - Velocity monitoring: ENABLED")
    print(f"   - Failed attempts: ENABLED")
    print(f"   - All 20 payment types")
    print(f"   - All 6 channels")
    print("\n" + "="*70)
    
    response = input("\n Start consumer? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    consumer = KafkaOnlyFraudConsumer(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        kafka_group_id=KAFKA_GROUP_ID,
        fraud_api_url=FRAUD_API_URL
    )
    
    consumer.start()


if __name__ == "__main__":
    main()

