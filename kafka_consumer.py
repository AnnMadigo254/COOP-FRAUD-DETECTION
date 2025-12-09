# """
# ENHANCED YEA Fraud Detection - Kafka Consumer
# Extracts ALL valuable features from Kafka + Customer API

# NEW FEATURES:
# ✅ device_id - Account takeover detection
# ✅ phone_number - SIM swap detection  
# ✅ debit_account - Velocity tracking
# ✅ status - Failed attempt patterns
# ✅ phone_number (API) - Cross-reference check
# ✅ income - Transaction-to-income ratio
# ✅ KYC date - Recency check
# """

# import json
# import logging
# import requests
# import xml.etree.ElementTree as ET
# from datetime import datetime, timedelta
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


# class CustomerAPIClient:
#     """Enhanced Client for Co-op Bank Customer SOAP API"""
    
#     def __init__(self, api_url: str, timeout: int = 10):
#         self.api_url = api_url
#         self.timeout = timeout
    
#     def get_customer_details(self, customer_id: str) -> Optional[Dict]:
#         """Get customer details from SOAP API with ALL fields"""
#         try:
#             soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
# <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
#                   xmlns:tns="urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0">
#    <soapenv:Header/>
#    <soapenv:Body>
#       <tns:RetCustomerInqReq>
#          <tns:CustomerId>{customer_id}</tns:CustomerId>
#       </tns:RetCustomerInqReq>
#    </soapenv:Body>
# </soapenv:Envelope>"""
            
#             headers = {
#                 'Content-Type': 'text/xml; charset=utf-8',
#                 'SOAPAction': 'urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0'
#             }
            
#             response = requests.post(
#                 self.api_url,
#                 data=soap_request,
#                 headers=headers,
#                 timeout=self.timeout
#             )
            
#             if response.status_code != 200:
#                 logger.error(f"Customer API returned {response.status_code}")
#                 return None
            
#             root = ET.fromstring(response.text)
            
#             namespaces = {
#                 'tns40': 'urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0'
#             }
            
#             basic_details = root.find('.//tns40:personalPartyBasicDetails', namespaces)
            
#             if basic_details is None:
#                 logger.error("No customer details found")
#                 return None
            
#             # Extract ALL useful fields
#             return {
#                 # Original fields
#                 'date_of_birth': self._get_text(basic_details, 'tns40:DateOfBirth', namespaces),
#                 'gender': self._get_text(basic_details, 'tns40:Gender', namespaces, 'F'),
#                 'relationship_opening_date': self._get_text(basic_details, 'tns40:RelationshipOpeningDate', namespaces, '2020-01-01'),
#                 'risk_rating': self._get_text(basic_details, 'tns40:RiskRating', namespaces, 'LOW'),
#                 'customer_status': self._get_text(basic_details, 'tns40:CustomerStatus', namespaces, 'ACTVE'),
#                 'full_name': self._get_text(basic_details, 'tns40:FullName', namespaces, 'Unknown'),
                
#                 # NEW: Enhanced fields
#                 'phone_number': self._get_text(basic_details, 'tns40:PhoneNo', namespaces),  # For SIM swap detection
#                 'income': self._get_text(basic_details, 'tns40:Income', namespaces),  # For income ratio
#                 'kyc_date': self._get_text(basic_details, 'tns40:KYCDate', namespaces),  # For KYC recency
#             }
            
#         except Exception as e:
#             logger.error(f"Error calling Customer API: {str(e)}")
#             return None
    
#     def _get_text(self, element, path, namespaces, default=None):
#         """Safely extract text from XML"""
#         found = element.find(path, namespaces)
#         return found.text if found is not None else default


# class EnhancedFraudDetectionConsumer:
#     """Enhanced Kafka consumer with full feature extraction"""
    
#     def __init__(
#         self,
#         kafka_bootstrap_servers: str,
#         kafka_topic: str,
#         kafka_group_id: str,
#         customer_api_url: str,
#         fraud_api_url: str
#     ):
#         self.kafka_bootstrap_servers = kafka_bootstrap_servers
#         self.kafka_topic = kafka_topic
#         self.kafka_group_id = kafka_group_id
#         self.customer_api_url = customer_api_url
#         self.fraud_api_url = fraud_api_url
        
#         self.customer_api = CustomerAPIClient(customer_api_url)
#         self.consumer = None
        
#         # NEW: Historical tracking for velocity features
#         self.device_history = defaultdict(set)  # customer_id -> set of device_ids
#         self.account_history = defaultdict(list)  # debit_account -> list of timestamps
#         self.failed_attempts = defaultdict(list)  # customer_id -> list of failed timestamps
        
#         # Statistics
#         self.stats = {
#             'total_processed': 0,
#             'fraud_detected': 0,
#             'blocked': 0,
#             'reviewed': 0,
#             'approved': 0,
#             'errors': 0,
#             'api_calls': 0,
#             # NEW stats
#             'device_switches': 0,
#             'sim_swaps_detected': 0,
#             'velocity_alerts': 0,
#             'failed_attempt_alerts': 0
#         }
    
#     def initialize_consumer(self) -> bool:
#         """Initialize Kafka consumer"""
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
#         """Determine if transaction should be checked"""
#         amount = float(transaction.get('transaction_amount', 0))
#         if amount == 0:
#             return False
        
#         payment_types = [
#             'PAY_TO_COOPTILL',
#             'POST_DIRECT_PAYMENTS', 
#             'PERSONAL_ELOAN',
#             'COOP_REMIT_POST',
#             'INSTITUTIONAL_PAYMENT_REQUEST'
#         ]
#         txn_type = transaction.get('transaction_type', '')
        
#         return txn_type in payment_types
    
#     def extract_enhanced_kafka_features(self, kafka_txn: Dict) -> Dict:
#         """
#         Extract ALL valuable features from Kafka message
        
#         Returns dict with enhanced features
#         """
#         customer_id = kafka_txn.get('customer_id')
#         now = datetime.now()
        
#         # === DEVICE FEATURES (NEW!) ===
#         device_id = kafka_txn.get('device_id', 'UNKNOWN')
        
#         # Check if device is new for this customer
#         known_devices = self.device_history.get(customer_id, set())
#         is_new_device = 1 if (device_id not in known_devices and device_id != 'UNKNOWN') else 0
        
#         if is_new_device:
#             self.stats['device_switches'] += 1
#             logger.warning(f"🔄 NEW DEVICE: Customer {customer_id}, Device: {device_id}")
        
#         # Track device
#         if device_id != 'UNKNOWN':
#             self.device_history[customer_id].add(device_id)
        
#         # === PHONE NUMBER (NEW!) ===
#         kafka_phone = kafka_txn.get('phone_number', '')
        
#         # === ACCOUNT VELOCITY (NEW!) ===
#         debit_account = kafka_txn.get('debit_account', '')
#         transactions_today = 0
        
#         if debit_account:
#             account_txns = self.account_history.get(debit_account, [])
#             # Filter to today
#             today_txns = [t for t in account_txns if (now - t).days == 0]
#             transactions_today = len(today_txns)
            
#             # Add current transaction
#             self.account_history[debit_account].append(now)
            
#             if transactions_today >= 5:
#                 self.stats['velocity_alerts'] += 1
#                 logger.warning(f"⚡ VELOCITY ALERT: Account {debit_account}, {transactions_today} transactions today")
        
#         # === FAILED ATTEMPTS (NEW!) ===
#         status = kafka_txn.get('status', 200)
        
#         # Track failed attempts
#         if status != 200:
#             self.failed_attempts[customer_id].append(now)
#             logger.warning(f"❌ FAILED: Customer {customer_id}, Status: {status}")
        
#         # Count recent failed attempts (last hour)
#         recent_failures = [
#             t for t in self.failed_attempts.get(customer_id, [])
#             if (now - t).seconds < 3600
#         ]
#         failed_attempts_last_hour = len(recent_failures)
        
#         if failed_attempts_last_hour >= 3:
#             self.stats['failed_attempt_alerts'] += 1
#             logger.critical(f"🚨 MULTIPLE FAILURES: Customer {customer_id}, {failed_attempts_last_hour} failed attempts")
        
#         return {
#             'device_id': device_id,
#             'is_new_device': is_new_device,
#             'kafka_phone': kafka_phone,
#             'debit_account': debit_account,
#             'transactions_today': transactions_today,
#             'status': status,
#             'failed_attempts_last_hour': failed_attempts_last_hour
#         }
    
#     def check_sim_swap(self, kafka_phone: str, api_phone: str, customer_id: str) -> int:
#         """Check for SIM swap fraud"""
#         if not kafka_phone or not api_phone:
#             return 0
        
#         # Normalize phone numbers
#         kafka_clean = kafka_phone.replace('+', '').replace(' ', '').replace('-', '')
#         api_clean = api_phone.replace('+', '').replace(' ', '').replace('-', '')
        
#         if kafka_clean != api_clean:
#             self.stats['sim_swaps_detected'] += 1
#             logger.critical(
#                 f"🚨🚨🚨 SIM SWAP DETECTED 🚨🚨🚨\n"
#                 f"   Customer: {customer_id}\n"
#                 f"   Kafka Phone: {kafka_phone}\n"
#                 f"   API Phone: {api_phone}\n"
#                 f"   ACTION: IMMEDIATE BLOCK"
#             )
#             return 1
        
#         return 0
    
#     def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
#         """Call fraud detection API"""
#         try:
#             self.stats['api_calls'] += 1
            
#             response = requests.post(
#                 self.fraud_api_url,
#                 json=transaction_data,
#                 timeout=5,
#                 headers={'Content-Type': 'application/json'}
#             )
            
#             if response.status_code == 200:
#                 return response.json()
#             else:
#                 logger.error(f"Fraud API error {response.status_code}: {response.text}")
#                 return None
                
#         except Exception as e:
#             logger.error(f"Error calling fraud API: {str(e)}")
#             return None
    
#     def handle_fraud_result(self, transaction: Dict, fraud_result: Dict, enhanced_features: Dict):
#         """Handle fraud detection result with enhanced logging"""
#         recommendation = fraud_result.get('recommendation', 'APPROVE')
#         risk_level = fraud_result.get('risk_level', 'UNKNOWN')
#         fraud_score = fraud_result.get('fraud_score', 0)
        
#         txn_id = transaction.get('reference') or transaction.get('Id', 'N/A')
#         customer_id = transaction.get('customer_id', 'N/A')
#         amount = transaction.get('transaction_amount', 0)
        
#         # Enhanced logging with new features
#         extra_info = []
#         if enhanced_features.get('is_new_device'):
#             extra_info.append("NEW_DEVICE")
#         if enhanced_features.get('phone_mismatch'):
#             extra_info.append("SIM_SWAP")
#         if enhanced_features.get('transactions_today', 0) > 5:
#             extra_info.append(f"HIGH_VELOCITY({enhanced_features['transactions_today']})")
#         if enhanced_features.get('failed_attempts_last_hour', 0) > 0:
#             extra_info.append(f"FAILED_ATTEMPTS({enhanced_features['failed_attempts_last_hour']})")
        
#         extra_str = f" | {', '.join(extra_info)}" if extra_info else ""
        
#         if recommendation == 'BLOCK':
#             self.stats['blocked'] += 1
#             logger.critical(
#                 f"🚨 BLOCK: TXN={txn_id}, Customer={customer_id}, "
#                 f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}"
#             )
            
#         elif recommendation == 'REVIEW':
#             self.stats['reviewed'] += 1
#             logger.warning(
#                 f"⚠️  REVIEW: TXN={txn_id}, Customer={customer_id}, "
#                 f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}"
#             )
            
#         else:  # APPROVE
#             self.stats['approved'] += 1
#             logger.info(
#                 f"✅ APPROVE: TXN={txn_id}, Amount={amount}, "
#                 f"Risk={risk_level}, Score={fraud_score:.2f}{extra_str}"
#             )
        
#         # Log to dashboard
#         self.log_to_dashboard(transaction, fraud_result)
    
#     def process_transaction(self, kafka_message: Dict):
#         """Process transaction with enhanced feature extraction"""
#         try:
#             transaction_id = kafka_message.get('reference') or kafka_message.get('Id', 'N/A')
#             customer_id = kafka_message.get('customer_id')
#             transaction_amount = kafka_message.get('transaction_amount', 0)
#             transaction_type = kafka_message.get('transaction_type', 'UNKNOWN')
            
#             logger.info(
#                 f"📥 Transaction: {transaction_id}, Customer: {customer_id}, "
#                 f"Amount: {transaction_amount}, Type: {transaction_type}"
#             )
            
#             # Check if we should analyze
#             if not self.should_check_fraud(kafka_message):
#                 logger.info(f"⏭️  Skipping non-payment: {transaction_type}")
#                 return
            
#             # Extract enhanced Kafka features
#             kafka_features = self.extract_enhanced_kafka_features(kafka_message)
            
#             # Get customer details
#             customer_details = self.customer_api.get_customer_details(customer_id)
            
#             if not customer_details:
#                 logger.error(f"❌ Failed to get customer details for {customer_id}")
#                 self.stats['errors'] += 1
#                 return
            
#             # Check for SIM swap
#             phone_mismatch = self.check_sim_swap(
#                 kafka_features['kafka_phone'],
#                 customer_details.get('phone_number', ''),
#                 customer_id
#             )
            
#             # If SIM swap detected, automatically BLOCK
#             if phone_mismatch:
#                 logger.critical(f"🚨 AUTO-BLOCK: SIM SWAP FRAUD DETECTED for {customer_id}")
#                 self.stats['blocked'] += 1
#                 self.stats['fraud_detected'] += 1
#                 return
            
#             # Prepare fraud detection request with ALL features
#             fraud_request = {
#                 # Original features
#                 'transaction_id': transaction_id,
#                 'customer_id': customer_id,
#                 'date_of_birth': customer_details['date_of_birth'],
#                 'gender': customer_details['gender'],
#                 'transaction_amount': str(transaction_amount),
#                 'transaction_date': kafka_message.get('transaction_date', datetime.now().isoformat()),
#                 'transaction_type': transaction_type,
#                 'channelId': kafka_message.get('channelId', '2E08D0409BE26BA3E064020820E70A68'),
#                 'relationship_opening_date': customer_details['relationship_opening_date'],
#                 'risk_rating': customer_details['risk_rating'],
#                 'customer_status': customer_details['customer_status'],
                
#                 # NEW: Enhanced features (for future model)
#                 'device_id': kafka_features['device_id'],
#                 'is_new_device': kafka_features['is_new_device'],
#                 'transactions_today': kafka_features['transactions_today'],
#                 'failed_attempts_last_hour': kafka_features['failed_attempts_last_hour'],
#             }
            
#             # Call fraud API
#             fraud_result = self.call_fraud_api(fraud_request)
            
#             if not fraud_result:
#                 logger.error(f"❌ Failed to get fraud prediction for {transaction_id}")
#                 self.stats['errors'] += 1
#                 return
            
#             # Update stats
#             self.stats['total_processed'] += 1
            
#             if fraud_result.get('fraud_prediction') == 1:
#                 self.stats['fraud_detected'] += 1
            
#             # Handle result
#             kafka_features['phone_mismatch'] = phone_mismatch
#             self.handle_fraud_result(kafka_message, fraud_result, kafka_features)
            
#         except Exception as e:
#             logger.error(f"❌ Error processing transaction: {str(e)}", exc_info=True)
#             self.stats['errors'] += 1
    
#     def print_stats(self):
#         """Print enhanced statistics"""
#         total = self.stats['total_processed']
#         fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
        
#         logger.info(f"\n{'='*70}")
#         logger.info(f"📊 ENHANCED STATISTICS")
#         logger.info(f"{'='*70}")
#         logger.info(f"Total Processed:  {total}")
#         logger.info(f"Fraud Detected:   {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
#         logger.info(f"  - Blocked:      {self.stats['blocked']}")
#         logger.info(f"  - Reviewed:     {self.stats['reviewed']}")
#         logger.info(f"  - Approved:     {self.stats['approved']}")
#         logger.info(f"\nEnhanced Detections:")
#         logger.info(f"  - Device Switches:     {self.stats['device_switches']}")
#         logger.info(f"  - SIM Swaps:           {self.stats['sim_swaps_detected']}")
#         logger.info(f"  - Velocity Alerts:     {self.stats['velocity_alerts']}")
#         logger.info(f"  - Failed Attempts:     {self.stats['failed_attempt_alerts']}")
#         logger.info(f"\nErrors:           {self.stats['errors']}")
#         logger.info(f"API Calls:        {self.stats['api_calls']}")
#         logger.info(f"{'='*70}\n")
    
#     def log_to_dashboard(self, transaction: Dict, fraud_result: Dict):
#         """Log to dashboard database"""
#         try:
#             conn = sqlite3.connect('fraud_events.db')
#             c = conn.cursor()
#             c.execute("""INSERT INTO fraud_events 
#                         (timestamp, transaction_id, customer_id, amount, transaction_type,
#                         fraud_prediction, risk_level, recommendation, fraud_score, processing_time_ms)
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
#                     (datetime.now().isoformat(),
#                     transaction.get('reference', 'N/A'),
#                     transaction.get('customer_id'),
#                     float(transaction.get('transaction_amount', 0)),
#                     transaction.get('transaction_type', 'UNKNOWN'),
#                     fraud_result.get('fraud_prediction'),
#                     fraud_result.get('risk_level'),
#                     fraud_result.get('recommendation'),
#                     fraud_result.get('fraud_score'),
#                     fraud_result.get('processing_time_ms', 0)))
#             conn.commit()
#             conn.close()
#         except Exception as e:
#             logger.error(f"Dashboard logging error: {e}")
    
#     def start(self):
#         """Start consuming messages"""
#         if not self.initialize_consumer():
#             logger.error("Failed to initialize consumer")
#             return
        
#         logger.info(f"\n{'='*70}")
#         logger.info(f"🚀 ENHANCED YEA FRAUD DETECTION - STARTED")
#         logger.info(f"{'='*70}")
#         logger.info(f"Kafka:    {self.kafka_bootstrap_servers}")
#         logger.info(f"Topic:    {self.kafka_topic}")
#         logger.info(f"Group:    {self.kafka_group_id}")
#         logger.info(f"\nNEW FEATURES ENABLED:")
#         logger.info(f"  ✅ Device tracking (account takeover)")
#         logger.info(f"  ✅ SIM swap detection")
#         logger.info(f"  ✅ Velocity monitoring")
#         logger.info(f"  ✅ Failed attempt tracking")
#         logger.info(f"{'='*70}\n")
        
#         try:
#             for message in self.consumer:
#                 try:
#                     transaction = message.value
#                     self.process_transaction(transaction)
                    
#                     # Print stats every 10 transactions
#                     if self.stats['total_processed'] % 10 == 0 and self.stats['total_processed'] > 0:
#                         self.print_stats()
                        
#                 except Exception as e:
#                     logger.error(f"Error handling message: {str(e)}", exc_info=True)
#                     continue
                    
#         except KeyboardInterrupt:
#             logger.info("\n⚠️  Shutting down...")
#         finally:
#             if self.consumer:
#                 self.consumer.close()
#             self.print_stats()
#             logger.info("✅ Consumer stopped")


# def main():
#     """Main entry point"""
    
#     KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76"
#     KAFKA_TOPIC = "YEA_Transaction_Monitoring"
#     KAFKA_GROUP_ID = "fraud_detection_consumer"
#     CUSTOMER_API_URL = "https://soapreprod2.co-opbank.co.ke/Customer/RetailCustomerInquiry/Get/2.0"
#     FRAUD_API_URL = "http://localhost:8000/fraud/predict"
    
#     print("\n" + "="*70)
#     print("🛡️  ENHANCED YEA FRAUD DETECTION CONSUMER")
#     print("="*70)
#     print(f"\n⚙️  Configuration:")
#     print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
#     print(f"   Topic: {KAFKA_TOPIC}")
#     print(f"   Customer API: {CUSTOMER_API_URL}")
#     print(f"   Fraud API: {FRAUD_API_URL}")
#     print(f"\n🆕 Enhanced Features:")
#     print(f"   ✅ Device ID tracking")
#     print(f"   ✅ SIM swap detection")
#     print(f"   ✅ Account velocity monitoring")
#     print(f"   ✅ Failed attempt detection")
#     print("\n" + "="*70)
    
#     response = input("\n✅ Start enhanced consumer? (y/n): ")
#     if response.lower() != 'y':
#         print("Cancelled.")
#         return
    
#     consumer = EnhancedFraudDetectionConsumer(
#         kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         kafka_topic=KAFKA_TOPIC,
#         kafka_group_id=KAFKA_GROUP_ID,
#         customer_api_url=CUSTOMER_API_URL,
#         fraud_api_url=FRAUD_API_URL
#     )
    
#     consumer.start()


# if __name__ == "__main__":
#     main()

"""
ROBUST YEA Fraud Detection - Kafka Consumer (KAFKA-ONLY MODE)
✅ NO CUSTOMER API DEPENDENCY
✅ Uses ONLY Kafka message fields
✅ Enhanced features: device_id, phone_number, debit_account, status
✅ Continues fraud detection even with minimal data
"""

import json
import logging
import requests
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, Optional
from collections import defaultdict
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaOnlyFraudDetectionConsumer:
    """Kafka-only consumer: no external dependencies"""

    # Real channelId values from Kafka (for one-hot encoding if needed)
    CHANNEL_IDS = [
        '2E08D0409BE26BA3E064020820E70A68',
        '2E08D0409BE76BA3E064020820E70A68',
        '2E08D0409BE16BA3E064020820E70A68',
        '2E08D0409BE46BA3E064020820E70A68'
    ]

    # Transaction types that trigger fraud check
    PAYMENT_TYPES = [
        'PAY_TO_COOPTILL',
        'POST_DIRECT_PAYMENTS',
        'PERSONAL_ELOAN',
        'COOP_REMIT_POST',
        'INSTITUTIONAL_PAYMENT_REQUEST'
    ]

    # Default fallback values (for missing optional fields)
    DEFAULTS = {
        'date_of_birth': '1990-01-01',
        'gender': 'F',
        'relationship_opening_date': '2023-01-01',
        'risk_rating': 'LOW',
        'customer_status': 'ACTVE'
    }

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

        # Historical tracking for velocity & device
        self.device_history = defaultdict(set)      # customer_id -> {device_id}
        self.account_history = defaultdict(list)    # debit_account -> [timestamps]
        self.failed_attempts = defaultdict(list)    # customer_id -> [timestamps]

        # Stats
        self.stats = {
            'total_processed': 0,
            'fraud_detected': 0,
            'blocked': 0,
            'reviewed': 0,
            'approved': 0,
            'errors': 0,
            'api_calls': 0,
            'device_switches': 0,
            'velocity_alerts': 0,
            'failed_attempt_alerts': 0
        }

    def initialize_consumer(self) -> bool:
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.kafka_group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"✅ Kafka consumer initialized: {self.kafka_topic}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka: {str(e)}")
            return False

    def should_check_fraud(self, transaction: Dict) -> bool:
        amount = float(transaction.get('transaction_amount', 0))
        if amount == 0:
            return False
        txn_type = transaction.get('transaction_type', '')
        return txn_type in self.PAYMENT_TYPES

    def extract_enhanced_features(self, txn: Dict) -> Dict:
        """Extract all available features from Kafka message with fallbacks"""
        from datetime import datetime, timedelta
        now = datetime.now()

        customer_id = txn.get('customer_id', 'N/A')
        device_id = txn.get('device_id', 'UNKNOWN')
        debit_account = txn.get('debit_account', '')
        status = txn.get('status', 200)
        phone_number = txn.get('phone_number', '')

        # === Device Tracking ===
        known_devices = self.device_history.get(customer_id, set())
        is_new_device = 1 if (device_id not in known_devices and device_id != 'UNKNOWN') else 0
        if is_new_device:
            self.stats['device_switches'] += 1
            logger.warning(f"🔄 NEW DEVICE: Customer {customer_id}, Device: {device_id}")
        if device_id != 'UNKNOWN':
            self.device_history[customer_id].add(device_id)

        # === Velocity (transactions per day per account) ===
        transactions_today = 0
        if debit_account:
            today_txns = [
                t for t in self.account_history[debit_account]
                if (now - t).days == 0
            ]
            transactions_today = len(today_txns)
            self.account_history[debit_account].append(now)
            if transactions_today >= 5:
                self.stats['velocity_alerts'] += 1
                logger.warning(f"⚡ VELOCITY ALERT: Account {debit_account}, {transactions_today} txns today")

        # === Failed Attempts (last hour) ===
        if status != 200:
            self.failed_attempts[customer_id].append(now)
        recent_failures = [
            t for t in self.failed_attempts.get(customer_id, [])
            if (now - t).seconds < 3600
        ]
        failed_attempts_last_hour = len(recent_failures)
        if failed_attempts_last_hour >= 3:
            self.stats['failed_attempt_alerts'] += 1
            logger.critical(f"🚨 MULTIPLE FAILURES: Customer {customer_id}, {failed_attempts_last_hour} attempts")

        return {
            'device_id': device_id,
            'is_new_device': is_new_device,
            'phone_number': phone_number,
            'debit_account': debit_account,
            'transactions_today': transactions_today,
            'failed_attempts_last_hour': failed_attempts_last_hour,
            'status': status
        }

    def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
        try:
            self.stats['api_calls'] += 1
            response = requests.post(
                self.fraud_api_url,
                json=transaction_data,
                timeout=5,
                headers={'Content-Type': 'application/json'}
            )
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error calling fraud API: {str(e)}")
            return None

    def handle_fraud_result(self, transaction: Dict, fraud_result: Dict, enhanced: Dict):
        recommendation = fraud_result.get('recommendation', 'APPROVE')
        risk_level = fraud_result.get('risk_level', 'UNKNOWN')
        fraud_score = fraud_result.get('fraud_score', 0)
        txn_id = transaction.get('reference') or transaction.get('Id', 'N/A')
        amount = transaction.get('transaction_amount', 0)

        # Build extra info string
        extra = []
        if enhanced.get('is_new_device'):
            extra.append("NEW_DEVICE")
        if enhanced.get('transactions_today', 0) >= 5:
            extra.append(f"HIGH_VELOCITY({enhanced['transactions_today']})")
        if enhanced.get('failed_attempts_last_hour', 0) > 0:
            extra.append(f"FAILED_ATTEMPTS({enhanced['failed_attempts_last_hour']})")
        extra_str = f" | {', '.join(extra)}" if extra else ""

        if recommendation == 'BLOCK':
            self.stats['blocked'] += 1
            logger.critical(f"🚨 BLOCK: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")
        elif recommendation == 'REVIEW':
            self.stats['reviewed'] += 1
            logger.warning(f"⚠️  REVIEW: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")
        else:
            self.stats['approved'] += 1
            logger.info(f"✅ APPROVE: TXN={txn_id}, Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}{extra_str}")

        self.log_to_dashboard(transaction, fraud_result)

    def process_transaction(self, kafka_message: Dict):
        try:
            transaction_id = kafka_message.get('reference') or kafka_message.get('Id', 'N/A')
            customer_id = kafka_message.get('customer_id', 'N/A')
            amount = kafka_message.get('transaction_amount', 0)
            txn_type = kafka_message.get('transaction_type', 'UNKNOWN')

            logger.info(f"📥 Transaction: {transaction_id}, Customer: {customer_id}, Amount: {amount}, Type: {txn_type}")

            if not self.should_check_fraud(kafka_message):
                logger.info(f"⏭️ Skipping non-payment: {txn_type}")
                return

            # Extract enhanced features from Kafka
            enhanced = self.extract_enhanced_features(kafka_message)

            # Build fraud request using ONLY Kafka data + defaults
            fraud_request = {
                # Required by API (with defaults)
                'transaction_id': transaction_id,
                'customer_id': customer_id,
                'date_of_birth': self.DEFAULTS['date_of_birth'],
                'gender': self.DEFAULTS['gender'],
                'transaction_amount': str(amount),
                'transaction_date': kafka_message.get('transaction_date', datetime.now().isoformat()),
                'transaction_type': txn_type,
                'channelId': kafka_message.get('channelId', self.CHANNEL_IDS[0]),
                'relationship_opening_date': self.DEFAULTS['relationship_opening_date'],
                'risk_rating': self.DEFAULTS['risk_rating'],
                'customer_status': self.DEFAULTS['customer_status'],

                # Enhanced Kafka features (for rule-based logic in API)
                'is_new_device': enhanced['is_new_device'],
                'transactions_today': enhanced['transactions_today'],
                'failed_attempts_last_hour': enhanced['failed_attempts_last_hour'],
                'phone_mismatch': 0,  # SIM swap requires Customer API → disabled in Kafka-only
            }

            fraud_result = self.call_fraud_api(fraud_request)
            if not fraud_result:
                logger.error(f"❌ Fraud API failed for {transaction_id}")
                self.stats['errors'] += 1
                return

            self.stats['total_processed'] += 1
            if fraud_result.get('fraud_prediction') == 1:
                self.stats['fraud_detected'] += 1

            self.handle_fraud_result(kafka_message, fraud_result, enhanced)

        except Exception as e:
            logger.error(f"❌ Error processing transaction: {str(e)}", exc_info=True)
            self.stats['errors'] += 1

    def log_to_dashboard(self, transaction: Dict, fraud_result: Dict):
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
        total = self.stats['total_processed']
        fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 KAFKA-ONLY STATISTICS")
        logger.info(f"{'='*70}")
        logger.info(f"Total Processed:  {total}")
        logger.info(f"Fraud Detected:   {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
        logger.info(f"  - Blocked:      {self.stats['blocked']}")
        logger.info(f"  - Reviewed:     {self.stats['reviewed']}")
        logger.info(f"  - Approved:     {self.stats['approved']}")
        logger.info(f"\nEnhanced Detections:")
        logger.info(f"  - Device Switches:     {self.stats['device_switches']}")
        logger.info(f"  - Velocity Alerts:     {self.stats['velocity_alerts']}")
        logger.info(f"  - Failed Attempts:     {self.stats['failed_attempt_alerts']}")
        logger.info(f"\nErrors:           {self.stats['errors']}")
        logger.info(f"API Calls:        {self.stats['api_calls']}")
        logger.info(f"{'='*70}\n")

    def start(self):
        if not self.initialize_consumer():
            logger.error("Failed to initialize consumer")
            return

        logger.info(f"\n{'='*70}")
        logger.info(f"🚀 KAFKA-ONLY FRAUD DETECTION - STARTED")
        logger.info(f"{'='*70}")
        logger.info(f"Kafka: {self.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.kafka_topic}")
        logger.info(f"Mode:  KAFKA-ONLY (NO Customer API)")
        logger.info(f"{'='*70}\n")

        try:
            for message in self.consumer:
                try:
                    self.process_transaction(message.value)
                    if self.stats['total_processed'] % 10 == 0 and self.stats['total_processed'] > 0:
                        self.print_stats()
                except Exception as e:
                    logger.error(f"Message handler error: {str(e)}", exc_info=True)
                    continue
        except KeyboardInterrupt:
            logger.info("\n⚠️  Shutting down...")
        finally:
            if self.consumer:
                self.consumer.close()
            self.print_stats()
            logger.info("✅ Consumer stopped")


def main():
    KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76:9092"
    KAFKA_TOPIC = "YEA_Transaction_Monitoring"
    KAFKA_GROUP_ID = "fraud_detection_consumer_kafka_only"
    FRAUD_API_URL = "http://localhost:8000/fraud/predict"

    print(f"\n{'='*70}")
    print("🛡️  KAFKA-ONLY FRAUD DETECTION CONSUMER")
    print(f"{'='*70}")
    print(f"\n⚙️  Configuration:")
    print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Fraud API: {FRAUD_API_URL}")
    print(f"\n✅ Mode: Kafka-Only (No Customer API)")
    print(f"✅ Enhanced Features: Device, Velocity, Failed Attempts")
    print(f"\n{'='*70}")

    response = input("\n✅ Start Kafka-only consumer? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return

    consumer = KafkaOnlyFraudDetectionConsumer(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        kafka_group_id=KAFKA_GROUP_ID,
        fraud_api_url=FRAUD_API_URL
    )
    consumer.start()


if __name__ == "__main__":
    main()