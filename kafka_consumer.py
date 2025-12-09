# """
# YEA Fraud Detection - Kafka Consumer
# Consumes transactions from Kafka, enriches with Customer API, detects fraud
# """

# import json
# import logging
# import requests
# import xml.etree.ElementTree as ET
# from datetime import datetime
# from kafka import KafkaConsumer
# from typing import Dict, Optional

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)


# class CustomerAPIClient:
#     """Client for Co-op Bank Customer SOAP API"""
    
#     def __init__(self, api_url: str, timeout: int = 10):
#         self.api_url = api_url
#         self.timeout = timeout
    
#     def get_customer_details(self, customer_id: str) -> Optional[Dict]:
#         """
#         Get customer details from SOAP API
        
#         Args:
#             customer_id: Customer ID
            
#         Returns:
#             Dict with customer details or None if error
#         """
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
            
#             # Parse XML response
#             root = ET.fromstring(response.text)
            
#             namespaces = {
#                 'tns40': 'urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0'
#             }
            
#             basic_details = root.find('.//tns40:personalPartyBasicDetails', namespaces)
            
#             if basic_details is None:
#                 logger.error("No customer details found in response")
#                 return None
            
#             # Extract fields
#             date_of_birth = basic_details.find('tns40:DateOfBirth', namespaces)
#             gender = basic_details.find('tns40:Gender', namespaces)
#             relationship_date = basic_details.find('tns40:RelationshipOpeningDate', namespaces)
#             risk_rating = basic_details.find('tns40:RiskRating', namespaces)
#             customer_status = basic_details.find('tns40:CustomerStatus', namespaces)
#             full_name = basic_details.find('tns40:FullName', namespaces)
            
#             return {
#                 'date_of_birth': date_of_birth.text if date_of_birth is not None else None,
#                 'gender': gender.text if gender is not None else 'F',
#                 'relationship_opening_date': relationship_date.text if relationship_date is not None else '2020-01-01',
#                 'risk_rating': risk_rating.text if risk_rating is not None else 'LOW',
#                 'customer_status': customer_status.text if customer_status is not None else 'ACTVE',
#                 'full_name': full_name.text if full_name is not None else 'Unknown'
#             }
            
#         except Exception as e:
#             logger.error(f"Error calling Customer API: {str(e)}")
#             return None


# class FraudDetectionConsumer:
#     """Kafka consumer for real-time fraud detection"""
    
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
        
#         # Statistics
#         self.stats = {
#             'total_processed': 0,
#             'fraud_detected': 0,
#             'blocked': 0,
#             'reviewed': 0,
#             'approved': 0,
#             'errors': 0,
#             'api_calls': 0
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
#         """
#         Determine if transaction should be checked for fraud
        
#         Args:
#             transaction: Transaction from Kafka
            
#         Returns:
#             True if should check, False if skip
#         """
#         # Skip balance checks and validations (amount = 0)
#         amount = float(transaction.get('transaction_amount', 0))
#         if amount == 0:
#             return False
        
#         # Only check actual payment transactions
#         payment_types = ['PAY_TO_COOPTILL', 'POST_DIRECT_PAYMENTS', 'PERSONAL_ELOAN']
#         txn_type = transaction.get('transaction_type', '')
        
#         return txn_type in payment_types
    
#     def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
#         """
#         Call fraud detection API
        
#         Args:
#             transaction_data: Complete transaction data
            
#         Returns:
#             Fraud detection result or None
#         """
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
    
#     def handle_fraud_result(self, transaction: Dict, fraud_result: Dict):
#         """
#         Handle fraud detection result
        
#         Args:
#             transaction: Original transaction
#             fraud_result: Fraud detection result
#         """
#         recommendation = fraud_result.get('recommendation', 'APPROVE')
#         risk_level = fraud_result.get('risk_level', 'UNKNOWN')
#         fraud_score = fraud_result.get('fraud_score', 0)
        
#         txn_id = transaction.get('reference') or transaction.get('Id', 'N/A')
#         customer_id = transaction.get('customer_id', 'N/A')
#         amount = transaction.get('transaction_amount', 0)
        
#         if recommendation == 'BLOCK':
#             self.stats['blocked'] += 1
#             logger.critical(
#                 f"🚨 BLOCK: TXN={txn_id}, Customer={customer_id}, "
#                 f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}"
#             )
#             # TODO: Call API to block transaction
            
#         elif recommendation == 'REVIEW':
#             self.stats['reviewed'] += 1
#             logger.warning(
#                 f"⚠️  REVIEW: TXN={txn_id}, Customer={customer_id}, "
#                 f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}"
#             )
#             # TODO: Send to review queue
            
#         else:  # APPROVE
#             self.stats['approved'] += 1
#             logger.info(
#                 f"✅ APPROVE: TXN={txn_id}, Amount={amount}, "
#                 f"Risk={risk_level}, Score={fraud_score:.2f}"
#             )
    
#     def process_transaction(self, kafka_message: Dict):
#         """
#         Process a single transaction
        
#         Args:
#             kafka_message: Transaction from Kafka
#         """
#         try:
#             # Extract transaction details
#             transaction_id = kafka_message.get('reference') or kafka_message.get('Id', 'N/A')
#             customer_id = kafka_message.get('customer_id')
#             transaction_amount = kafka_message.get('transaction_amount', 0)
#             transaction_type = kafka_message.get('transaction_type', 'UNKNOWN')
            
#             logger.info(
#                 f"📥 Transaction: {transaction_id}, Customer: {customer_id}, "
#                 f"Amount: {transaction_amount}, Type: {transaction_type}"
#             )
            
#             # Check if we should analyze this transaction
#             if not self.should_check_fraud(kafka_message):
#                 logger.info(f"⏭️  Skipping non-payment transaction: {transaction_type}")
#                 return
            
#             # Get customer details
#             customer_details = self.customer_api.get_customer_details(customer_id)
            
#             if not customer_details:
#                 logger.error(f"❌ Failed to get customer details for {customer_id}")
#                 self.stats['errors'] += 1
#                 return
            
#             # Prepare fraud detection request
#             fraud_request = {
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
#                 'customer_status': customer_details['customer_status']
#             }
            
#             # Call fraud detection API
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
#             self.handle_fraud_result(kafka_message, fraud_result)
            
#         except Exception as e:
#             logger.error(f"❌ Error processing transaction: {str(e)}", exc_info=True)
#             self.stats['errors'] += 1
    
#     def print_stats(self):
#         """Print current statistics"""
#         total = self.stats['total_processed']
#         fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
        
#         logger.info(f"\n{'='*70}")
#         logger.info(f"📊 STATISTICS")
#         logger.info(f"{'='*70}")
#         logger.info(f"Total Processed:  {total}")
#         logger.info(f"Fraud Detected:   {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
#         logger.info(f"  - Blocked:      {self.stats['blocked']}")
#         logger.info(f"  - Reviewed:     {self.stats['reviewed']}")
#         logger.info(f"  - Approved:     {self.stats['approved']}")
#         logger.info(f"Errors:           {self.stats['errors']}")
#         logger.info(f"API Calls:        {self.stats['api_calls']}")
#         logger.info(f"{'='*70}\n")
    
#     def start(self):
#         """Start consuming messages"""
#         if not self.initialize_consumer():
#             logger.error("Failed to initialize consumer")
#             return
        
#         logger.info(f"\n{'='*70}")
#         logger.info(f"🚀 YEA FRAUD DETECTION - STARTED")
#         logger.info(f"{'='*70}")
#         logger.info(f"Kafka:    {self.kafka_bootstrap_servers}")
#         logger.info(f"Topic:    {self.kafka_topic}")
#         logger.info(f"Group:    {self.kafka_group_id}")
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
    
#     # Configuration
#     KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76"
#     KAFKA_TOPIC = "YEA_Transaction_Monitoring"
#     KAFKA_GROUP_ID = "fraud_detection_consumer"
    
#     # TODO: Update these URLs with your actual endpoints
#     # CUSTOMER_API_URL = "http://your-esb-url/customer/inquiry"  # Update this!
#     CUSTOMER_API_URL = "https://soapreprod2.co-opbank.co.ke/Customer/RetailCustomerInquiry/Get/2.0"
#     FRAUD_API_URL = "http://localhost:8000/fraud/predict"
    
#     print("\n" + "="*70)
#     print("YEA FRAUD DETECTION CONSUMER")
#     print("="*70)
#     print(f"\n⚙️  Configuration:")
#     print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
#     print(f"   Topic: {KAFKA_TOPIC}")
#     print(f"   Customer API: {CUSTOMER_API_URL}")
#     print(f"   Fraud API: {FRAUD_API_URL}")
#     print("\n" + "="*70)
    
#     # Confirm before starting
#     response = input("\n✅ Configuration correct? Start consumer? (y/n): ")
#     if response.lower() != 'y':
#         print("Cancelled.")
#         return
    
#     # Create and start consumer
#     consumer = FraudDetectionConsumer(
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
YEA Fraud Detection - Kafka Consumer
Consumes transactions from Kafka, enriches with Customer API, detects fraud
"""

import json
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomerAPIClient:
    """Client for Co-op Bank Customer SOAP API"""
    
    def __init__(self, api_url: str, timeout: int = 10):
        self.api_url = api_url
        self.timeout = timeout
    
    def get_customer_details(self, customer_id: str) -> Optional[Dict]:
        """
        Get customer details from SOAP API
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Dict with customer details or None if error
        """
        try:
            soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:tns="urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0">
   <soapenv:Header/>
   <soapenv:Body>
      <tns:RetCustomerInqReq>
         <tns:CustomerId>{customer_id}</tns:CustomerId>
      </tns:RetCustomerInqReq>
   </soapenv:Body>
</soapenv:Envelope>"""
            
            headers = {
                'Content-Type': 'text/xml; charset=utf-8',
                'SOAPAction': 'urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0'
            }
            
            response = requests.post(
                self.api_url,
                data=soap_request,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                logger.error(f"Customer API returned {response.status_code}")
                return None
            
            # Parse XML response
            root = ET.fromstring(response.text)
            
            namespaces = {
                'tns40': 'urn://co-opbank.co.ke/BS/Customer/RetailCustomerInquiry.Get.2.0'
            }
            
            basic_details = root.find('.//tns40:personalPartyBasicDetails', namespaces)
            
            if basic_details is None:
                logger.error("No customer details found in response")
                return None
            
            # Extract fields
            date_of_birth = basic_details.find('tns40:DateOfBirth', namespaces)
            gender = basic_details.find('tns40:Gender', namespaces)
            relationship_date = basic_details.find('tns40:RelationshipOpeningDate', namespaces)
            risk_rating = basic_details.find('tns40:RiskRating', namespaces)
            customer_status = basic_details.find('tns40:CustomerStatus', namespaces)
            full_name = basic_details.find('tns40:FullName', namespaces)
            
            return {
                'date_of_birth': date_of_birth.text if date_of_birth is not None else None,
                'gender': gender.text if gender is not None else 'F',
                'relationship_opening_date': relationship_date.text if relationship_date is not None else '2020-01-01',
                'risk_rating': risk_rating.text if risk_rating is not None else 'LOW',
                'customer_status': customer_status.text if customer_status is not None else 'ACTVE',
                'full_name': full_name.text if full_name is not None else 'Unknown'
            }
            
        except Exception as e:
            logger.error(f"Error calling Customer API: {str(e)}")
            return None


class FraudDetectionConsumer:
    """Kafka consumer for real-time fraud detection"""
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        kafka_group_id: str,
        customer_api_url: str,
        fraud_api_url: str
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.customer_api_url = customer_api_url
        self.fraud_api_url = fraud_api_url
        
        self.customer_api = CustomerAPIClient(customer_api_url)
        self.consumer = None
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'fraud_detected': 0,
            'blocked': 0,
            'reviewed': 0,
            'approved': 0,
            'errors': 0,
            'api_calls': 0
        }
    
    def initialize_consumer(self) -> bool:
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.kafka_group_id,
                auto_offset_reset='earliest',  # ← Changed to read from beginning
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"✅ Kafka consumer initialized: {self.kafka_topic}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka: {str(e)}")
            return False
    
    def should_check_fraud(self, transaction: Dict) -> bool:
        """
        Determine if transaction should be checked for fraud
        
        Args:
            transaction: Transaction from Kafka
            
        Returns:
            True if should check, False if skip
        """
        # Skip balance checks and validations (amount = 0)
        amount = float(transaction.get('transaction_amount', 0))
        if amount == 0:
            return False
        
        # Only check actual payment transactions
        payment_types = [
            'PAY_TO_COOPTILL',
            'POST_DIRECT_PAYMENTS', 
            'PERSONAL_ELOAN',
            'COOP_REMIT_POST',  # New: Found in production
            'INSTITUTIONAL_PAYMENT_REQUEST'  # New: Found in production
        ]
        txn_type = transaction.get('transaction_type', '')
        
        return txn_type in payment_types
    
    def call_fraud_api(self, transaction_data: Dict) -> Optional[Dict]:
        """
        Call fraud detection API
        
        Args:
            transaction_data: Complete transaction data
            
        Returns:
            Fraud detection result or None
        """
        try:
            self.stats['api_calls'] += 1
            
            response = requests.post(
                self.fraud_api_url,
                json=transaction_data,
                timeout=5,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Fraud API error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling fraud API: {str(e)}")
            return None
    
    def handle_fraud_result(self, transaction: Dict, fraud_result: Dict):
        """
        Handle fraud detection result
        
        Args:
            transaction: Original transaction
            fraud_result: Fraud detection result
        """
        recommendation = fraud_result.get('recommendation', 'APPROVE')
        risk_level = fraud_result.get('risk_level', 'UNKNOWN')
        fraud_score = fraud_result.get('fraud_score', 0)
        
        txn_id = transaction.get('reference') or transaction.get('Id', 'N/A')
        customer_id = transaction.get('customer_id', 'N/A')
        amount = transaction.get('transaction_amount', 0)
        
        if recommendation == 'BLOCK':
            self.stats['blocked'] += 1
            logger.critical(
                f"🚨 BLOCK: TXN={txn_id}, Customer={customer_id}, "
                f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}"
            )
            # TODO: Call API to block transaction
            
        elif recommendation == 'REVIEW':
            self.stats['reviewed'] += 1
            logger.warning(
                f"⚠️  REVIEW: TXN={txn_id}, Customer={customer_id}, "
                f"Amount={amount}, Risk={risk_level}, Score={fraud_score:.2f}"
            )
            # TODO: Send to review queue
            
        else:  # APPROVE
            self.stats['approved'] += 1
            logger.info(
                f"✅ APPROVE: TXN={txn_id}, Amount={amount}, "
                f"Risk={risk_level}, Score={fraud_score:.2f}"
            )
    
    def process_transaction(self, kafka_message: Dict):
        """
        Process a single transaction
        
        Args:
            kafka_message: Transaction from Kafka
        """
        try:
            # Extract transaction details
            transaction_id = kafka_message.get('reference') or kafka_message.get('Id', 'N/A')
            customer_id = kafka_message.get('customer_id')
            transaction_amount = kafka_message.get('transaction_amount', 0)
            transaction_type = kafka_message.get('transaction_type', 'UNKNOWN')
            
            logger.info(
                f"📥 Transaction: {transaction_id}, Customer: {customer_id}, "
                f"Amount: {transaction_amount}, Type: {transaction_type}"
            )
            
            # Check if we should analyze this transaction
            if not self.should_check_fraud(kafka_message):
                logger.info(f"⏭️  Skipping non-payment transaction: {transaction_type}")
                return
            
            # Get customer details
            customer_details = self.customer_api.get_customer_details(customer_id)
            
            if not customer_details:
                logger.error(f"❌ Failed to get customer details for {customer_id}")
                self.stats['errors'] += 1
                return
            
            # Prepare fraud detection request
            fraud_request = {
                'transaction_id': transaction_id,
                'customer_id': customer_id,
                'date_of_birth': customer_details['date_of_birth'],
                'gender': customer_details['gender'],
                'transaction_amount': str(transaction_amount),
                'transaction_date': kafka_message.get('transaction_date', datetime.now().isoformat()),
                'transaction_type': transaction_type,
                'channelId': kafka_message.get('channelId', '2E08D0409BE26BA3E064020820E70A68'),
                'relationship_opening_date': customer_details['relationship_opening_date'],
                'risk_rating': customer_details['risk_rating'],
                'customer_status': customer_details['customer_status']
            }
            
            # Call fraud detection API
            fraud_result = self.call_fraud_api(fraud_request)
            
            if not fraud_result:
                logger.error(f"❌ Failed to get fraud prediction for {transaction_id}")
                self.stats['errors'] += 1
                return
            
            # Update stats
            self.stats['total_processed'] += 1
            
            if fraud_result.get('fraud_prediction') == 1:
                self.stats['fraud_detected'] += 1
            
            # Handle result
            self.handle_fraud_result(kafka_message, fraud_result)
            
        except Exception as e:
            logger.error(f"❌ Error processing transaction: {str(e)}", exc_info=True)
            self.stats['errors'] += 1
    
    def print_stats(self):
        """Print current statistics"""
        total = self.stats['total_processed']
        fraud_rate = (self.stats['fraud_detected'] / total * 100) if total > 0 else 0
        
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 STATISTICS")
        logger.info(f"{'='*70}")
        logger.info(f"Total Processed:  {total}")
        logger.info(f"Fraud Detected:   {self.stats['fraud_detected']} ({fraud_rate:.2f}%)")
        logger.info(f"  - Blocked:      {self.stats['blocked']}")
        logger.info(f"  - Reviewed:     {self.stats['reviewed']}")
        logger.info(f"  - Approved:     {self.stats['approved']}")
        logger.info(f"Errors:           {self.stats['errors']}")
        logger.info(f"API Calls:        {self.stats['api_calls']}")
        logger.info(f"{'='*70}\n")
    
    def start(self):
        """Start consuming messages"""
        if not self.initialize_consumer():
            logger.error("Failed to initialize consumer")
            return
        
        logger.info(f"\n{'='*70}")
        logger.info(f"🚀 YEA FRAUD DETECTION - STARTED")
        logger.info(f"{'='*70}")
        logger.info(f"Kafka:    {self.kafka_bootstrap_servers}")
        logger.info(f"Topic:    {self.kafka_topic}")
        logger.info(f"Group:    {self.kafka_group_id}")
        logger.info(f"{'='*70}\n")
        
        try:
            for message in self.consumer:
                try:
                    transaction = message.value
                    self.process_transaction(transaction)
                    
                    # Print stats every 10 transactions
                    if self.stats['total_processed'] % 10 == 0 and self.stats['total_processed'] > 0:
                        self.print_stats()
                        
                except Exception as e:
                    logger.error(f"Error handling message: {str(e)}", exc_info=True)
                    continue
                    
        except KeyboardInterrupt:
            logger.info("\n⚠️  Shutting down...")
        finally:
            if self.consumer:
                self.consumer.close()
            self.print_stats()
            logger.info("✅ Consumer stopped")


def main():
    """Main entry point"""
    
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "172.16.20.76"
    KAFKA_TOPIC = "YEA_Transaction_Monitoring"
    KAFKA_GROUP_ID = "fraud_detection_consumer"
    
    # TODO: Update these URLs with your actual endpoints
    CUSTOMER_API_URL = "https://soapreprod2.co-opbank.co.ke/Customer/RetailCustomerInquiry/Get/2.0"  # Update this!
    FRAUD_API_URL = "http://localhost:8000/fraud/predict"
    
    print("\n" + "="*70)
    print("YEA FRAUD DETECTION CONSUMER")
    print("="*70)
    print(f"\n⚙️  Configuration:")
    print(f"   Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Customer API: {CUSTOMER_API_URL}")
    print(f"   Fraud API: {FRAUD_API_URL}")
    print("\n" + "="*70)
    
    # Confirm before starting
    response = input("\n✅ Configuration correct? Start consumer? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Create and start consumer
    consumer = FraudDetectionConsumer(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        kafka_group_id=KAFKA_GROUP_ID,
        customer_api_url=CUSTOMER_API_URL,
        fraud_api_url=FRAUD_API_URL
    )
    
    consumer.start()


if __name__ == "__main__":
    main()