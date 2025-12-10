"""
YEA Kafka Data Extraction for Model Training - Kafka-Only Version
Extracts payment transaction data from Kafka topic for fraud detection model training.

Features extracted:
- Basic transaction features (5)
- Transaction type one-hot encoding (20)
- Channel one-hot encoding (6)
- Additional Kafka-derived features (6)

Total: 37 features

No Customer API dependency - uses only Kafka message fields.
"""

import json
import logging
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from typing import Dict, Optional
from collections import defaultdict
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaOnlyDataExtractor:
    """Extract training data using only Kafka fields"""
    
    # Skip these - not fraud relevant (96.4% of messages)
    SKIP_TYPES = [
        'GET_ACCOUNT_BALANCE',
        'BALANCEENQUIRY',
        'LIMITS_VALIDATION'
    ]
    
    # All 20 payment types for fraud detection
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
    
    # All 6 channels found in analysis
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
        output_file: str = 'yea_kafka_only_data.csv',
        max_messages: int = 100000
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.output_file = output_file
        self.max_messages = max_messages
        
        self.data = []
        self.stats = {
            'total_processed': 0,
            'payment_transactions': 0,
            'skipped_balance_checks': 0,
            'skipped_no_amount': 0
        }
    
    def extract_features(self, kafka_msg: Dict) -> Optional[Dict]:
        """Extract all features from Kafka message only"""
        
        try:
            # Transaction type check
            txn_type = kafka_msg.get('transaction_type', 'UNKNOWN')
            
            # Skip balance checks and validations
            if txn_type in self.SKIP_TYPES:
                self.stats['skipped_balance_checks'] += 1
                return None
            
            # Amount validation
            amount_str = kafka_msg.get('transaction_amount', '0')
            try:
                amount = float(amount_str)
            except:
                amount = 0.0
            
            # Skip zero amounts
            if amount == 0:
                self.stats['skipped_no_amount'] += 1
                return None
            
            # Time features extraction
            try:
                txn_date = kafka_msg.get('transaction_date', '')
                if txn_date:
                    dt = datetime.strptime(txn_date, '%Y-%m-%d %H:%M:%S')
                else:
                    dt = datetime.now()
                
                hour = dt.hour
                day_of_week = dt.weekday()
                is_weekend = 1 if day_of_week >= 5 else 0
                is_night = 1 if (hour >= 22 or hour <= 5) else 0
            except:
                hour = 12
                day_of_week = 3
                is_weekend = 0
                is_night = 0
            
            # Channel and device information
            channel_id = kafka_msg.get('channelId', self.CHANNEL_IDS[0])
            channel = kafka_msg.get('channel', '12')
            device_id = kafka_msg.get('device_id', 'UNKNOWN')
            session_id = kafka_msg.get('sessionId', 'UNKNOWN')
            
            # Customer and phone information
            phone_number = kafka_msg.get('phone_number', '')
            customer_id = kafka_msg.get('customer_id', '')
            
            # Account information
            debit_account = kafka_msg.get('debit_account', '')
            credit_account = kafka_msg.get('credit_account_id', '')
            
            # Transaction status
            status = kafka_msg.get('status', 1)
            try:
                status = int(status)
            except:
                status = 1
            
            # Currency information
            currency = kafka_msg.get('transaction_currency', 'KES')
            is_foreign_currency = 1 if currency != 'KES' else 0
            
            # Build feature vector
            features = {
                'amount': amount,
                'hour_of_day': hour,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'is_night': is_night,
            }
            
            # Transaction type one-hot encoding
            for tt in self.PAYMENT_TYPES:
                features[f'txn_{tt}'] = 1 if txn_type == tt else 0
            
            # Channel one-hot encoding
            for ch in self.CHANNEL_IDS:
                features[f'ch_{ch}'] = 1 if channel_id == ch else 0
            
            # Additional Kafka-derived features
            features['status_failed'] = 1 if status == 0 else 0
            features['is_foreign_currency'] = is_foreign_currency
            features['has_credit_account'] = 1 if credit_account else 0
            features['channel_numeric'] = int(channel) if channel.isdigit() else 12
            features['device_hash'] = hash(device_id) % 1000
            features['unique_phone'] = hash(phone_number) % 100
            
            # Fraud label (initially 0 - unsupervised learning)
            features['fraud'] = 0
            
            # Metadata for reference
            features['transaction_id'] = kafka_msg.get('id', 'N/A')
            features['customer_id'] = customer_id
            features['transaction_type'] = txn_type
            features['transaction_date'] = txn_date
            features['reference'] = kafka_msg.get('reference', '')
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting features: {str(e)}")
            return None
    
    def extract_data(self):
        """Extract payment transactions from Kafka topic"""
        
        logger.info("="*70)
        logger.info("KAFKA DATA EXTRACTION - STARTING")
        logger.info("="*70)
        logger.info(f"Kafka Server: {self.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.kafka_topic}")
        logger.info(f"Max Messages: {self.max_messages:,}")
        logger.info(f"Output File: {self.output_file}")
        logger.info("")
        logger.info("Target: Payment transactions only")
        logger.info("Skipping: Balance checks and validations")
        logger.info(f"Expected: ~3.6% of messages are payments")
        logger.info(f"Estimated payments: ~{int(self.max_messages * 0.036):,}")
        logger.info("="*70)
        logger.info("")
        
        # Initialize Kafka consumer
        try:
            consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=60000
            )
            
            logger.info("Connected to Kafka successfully")
            logger.info("")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            return
        
        logger.info("Reading messages from topic...")
        logger.info("")
        
        start_time = time.time()
        
        try:
            for message in consumer:
                if self.stats['total_processed'] >= self.max_messages:
                    logger.info("")
                    logger.info("Reached maximum message limit")
                    break
                
                try:
                    kafka_msg = message.value
                    self.stats['total_processed'] += 1
                    
                    # Extract features
                    features = self.extract_features(kafka_msg)
                    
                    if features:
                        self.data.append(features)
                        self.stats['payment_transactions'] += 1
                    
                    # Progress reporting
                    if self.stats['total_processed'] % 1000 == 0:
                        payment_pct = (self.stats['payment_transactions'] / 
                                     self.stats['total_processed'] * 100)
                        logger.info(
                            f"Progress: {self.stats['total_processed']:,} messages processed, "
                            f"{self.stats['payment_transactions']:,} payments extracted "
                            f"({payment_pct:.1f}%)"
                        )
                
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
        
        except Exception as e:
            logger.info("")
            logger.info(f"Reached end of topic or timeout")
        
        finally:
            consumer.close()
        
        elapsed = time.time() - start_time
        
        # Print final statistics
        self._print_results(elapsed)
    
    def _print_results(self, elapsed: float):
        """Print extraction results and save data"""
        
        logger.info("")
        logger.info("="*70)
        logger.info("EXTRACTION COMPLETE")
        logger.info("="*70)
        logger.info(f"Total messages read: {self.stats['total_processed']:,}")
        logger.info(f"Payment transactions: {self.stats['payment_transactions']:,}")
        logger.info(f"Skipped (balance checks): {self.stats['skipped_balance_checks']:,}")
        logger.info(f"Skipped (no amount): {self.stats['skipped_no_amount']:,}")
        
        if self.stats['total_processed'] > 0:
            payment_pct = self.stats['payment_transactions'] / self.stats['total_processed'] * 100
            logger.info(f"Payment rate: {payment_pct:.2f}%")
        
        logger.info(f"Time elapsed: {elapsed:.1f}s")
        logger.info("="*70)
        logger.info("")
        
        # Save to CSV
        if self.data:
            df = pd.DataFrame(self.data)
            df.to_csv(self.output_file, index=False)
            
            logger.info(f"Data saved to: {self.output_file}")
            logger.info(f"Shape: {df.shape}")
            logger.info(f"Features: {df.shape[1] - 5} (plus 5 metadata columns)")
            
            # Show feature columns
            feature_cols = [col for col in df.columns if col not in [
                'fraud', 'transaction_id', 'customer_id', 'transaction_type', 
                'transaction_date', 'reference'
            ]]
            
            logger.info("")
            logger.info(f"Feature columns ({len(feature_cols)}):")
            for i, col in enumerate(feature_cols[:20], 1):
                print(f"   {i:2d}. {col}")
            if len(feature_cols) > 20:
                print(f"   ... and {len(feature_cols) - 20} more")
            
            # Show sample data
            logger.info("")
            logger.info("Sample data (first 3 rows):")
            print(df[feature_cols[:10]].head(3).to_string())
            
            # Show transaction type distribution
            logger.info("")
            logger.info("Transaction Types in Dataset:")
            txn_counts = df['transaction_type'].value_counts()
            for txn_type, count in txn_counts.items():
                pct = count / len(df) * 100
                print(f"   {txn_type:40s} | {count:5,} ({pct:5.1f}%)")
            
            # Show fraud distribution
            logger.info("")
            logger.info("Fraud labels (should be 0 initially for unsupervised learning):")
            print(df['fraud'].value_counts())
            
        else:
            logger.warning("No payment transactions extracted")
            logger.warning("Possible reasons:")
            logger.warning("  1. All messages were balance checks/validations")
            logger.warning("  2. No messages with non-zero amounts")
            logger.warning("  3. Kafka topic has no payment data")


def main():
    """Main entry point"""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract Kafka training data for fraud detection model'
    )
    parser.add_argument('--server', default='172.16.20.76:9092', 
                       help='Kafka bootstrap server')
    parser.add_argument('--topic', default='YEA_Transaction_Monitoring', 
                       help='Kafka topic name')
    parser.add_argument('--max', type=int, default=100000, 
                       help='Maximum messages to process')
    parser.add_argument('--output', default='yea_kafka_only_data.csv', 
                       help='Output CSV filename')
    
    args = parser.parse_args()
    
    print("")
    print("="*70)
    print("KAFKA DATA EXTRACTION FOR FRAUD DETECTION MODEL TRAINING")
    print("="*70)
    print("")
    print("Configuration:")
    print(f"   Kafka Server: {args.server}")
    print(f"   Topic: {args.topic}")
    print(f"   Max Messages: {args.max:,}")
    print(f"   Output File: {args.output}")
    print("")
    print("Mode: Kafka-Only (no Customer API)")
    print("   - 37 features from Kafka")
    print("   - Fast extraction")
    print("   - No external dependencies")
    print("")
    print("="*70)
    
    response = input("\nStart extraction? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    extractor = KafkaOnlyDataExtractor(
        kafka_bootstrap_servers=args.server,
        kafka_topic=args.topic,
        output_file=args.output,
        max_messages=args.max
    )
    
    extractor.extract_data()
    
    print("")
    print("="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print("")
    print("Next steps:")
    print(f"1. Review {args.output}")
    print("2. Train model: python train_kafka_only_model.py")
    print("3. Deploy model to production")
    print("")
    print("="*70)


if __name__ == "__main__":
    main()