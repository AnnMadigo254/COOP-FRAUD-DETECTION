"""
View ALL Data in Kafka Topic - Comprehensive Analysis

This script reads ALL messages from the Kafka topic and shows you:
1. Complete message structure
2. All available fields
3. Field value distributions
4. Sample messages
5. Data quality statistics
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from collections import defaultdict, Counter
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaDataAnalyzer:
    """Analyze all data in Kafka topic"""
    
    def __init__(self, kafka_bootstrap_servers: str, kafka_topic: str, max_messages: int = None):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.max_messages = max_messages
        
        self.messages = []
        self.all_fields = set()
        self.field_stats = defaultdict(lambda: {
            'count': 0,
            'non_null': 0,
            'null': 0,
            'sample_values': [],
            'unique_values': set()
        })
        
        self.stats = {
            'total_messages': 0,
            'transaction_types': Counter(),
            'channels': Counter(),
            'statuses': Counter(),
            'amounts_range': {'min': float('inf'), 'max': 0},
            'customers': set(),
            'devices': set(),
            'accounts': set()
        }
    
    def analyze_topic(self):
        """Read and analyze all messages from Kafka topic"""
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 ANALYZING KAFKA TOPIC: {self.kafka_topic}")
        logger.info(f"{'='*80}")
        logger.info(f"Server: {self.kafka_bootstrap_servers}")
        logger.info(f"Max messages: {self.max_messages if self.max_messages else 'ALL'}")
        logger.info(f"{'='*80}\n")
        
        # Create consumer starting from earliest
        try:
            consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=30000  # 30 second timeout
            )
            
            logger.info("✅ Connected to Kafka\n")
            logger.info("📥 Reading messages (this may take a while)...\n")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect: {str(e)}")
            return
        
        start_time = time.time()
        
        try:
            for message in consumer:
                if self.max_messages and self.stats['total_messages'] >= self.max_messages:
                    logger.info(f"\n✅ Reached max messages limit")
                    break
                
                try:
                    msg = message.value
                    self.messages.append(msg)
                    self.stats['total_messages'] += 1
                    
                    # Analyze message
                    self._analyze_message(msg)
                    
                    # Progress
                    if self.stats['total_messages'] % 500 == 0:
                        logger.info(f"Progress: {self.stats['total_messages']} messages analyzed...")
                
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
        
        except Exception as e:
            logger.info(f"\n⏱️  Reached end of topic or timeout")
        
        finally:
            consumer.close()
        
        elapsed = time.time() - start_time
        
        # Print results
        self._print_analysis(elapsed)
    
    def _analyze_message(self, msg: dict):
        """Analyze a single message"""
        
        # Collect all fields
        for key in msg.keys():
            self.all_fields.add(key)
            
            value = msg.get(key)
            self.field_stats[key]['count'] += 1
            
            if value is not None and value != '':
                self.field_stats[key]['non_null'] += 1
                
                # Sample values (keep first 5)
                if len(self.field_stats[key]['sample_values']) < 5:
                    self.field_stats[key]['sample_values'].append(value)
                
                # Unique values (for small sets)
                if len(self.field_stats[key]['unique_values']) < 100:
                    self.field_stats[key]['unique_values'].add(str(value))
            else:
                self.field_stats[key]['null'] += 1
        
        # Transaction type
        txn_type = msg.get('transaction_type')
        if txn_type:
            self.stats['transaction_types'][txn_type] += 1
        
        # Channel
        channel = msg.get('channelId')
        if channel:
            self.stats['channels'][channel] += 1
        
        # Status
        status = msg.get('status')
        if status:
            self.stats['statuses'][status] += 1
        
        # Amount
        amount = msg.get('transaction_amount')
        if amount:
            try:
                amt = float(amount)
                self.stats['amounts_range']['min'] = min(self.stats['amounts_range']['min'], amt)
                self.stats['amounts_range']['max'] = max(self.stats['amounts_range']['max'], amt)
            except:
                pass
        
        # Customer
        customer = msg.get('customer_id')
        if customer:
            self.stats['customers'].add(customer)
        
        # Device
        device = msg.get('device_id')
        if device:
            self.stats['devices'].add(device)
        
        # Account
        account = msg.get('debit_account')
        if account:
            self.stats['accounts'].add(account)
    
    def _print_analysis(self, elapsed: float):
        """Print comprehensive analysis"""
        
        print(f"\n{'='*80}")
        print(f"📊 KAFKA TOPIC ANALYSIS COMPLETE")
        print(f"{'='*80}")
        print(f"Total Messages:     {self.stats['total_messages']:,}")
        print(f"Analysis Time:      {elapsed:.1f}s")
        print(f"Messages/sec:       {self.stats['total_messages']/elapsed:.1f}")
        print(f"{'='*80}\n")
        
        # === FIELD STRUCTURE ===
        print(f"{'='*80}")
        print(f"📋 ALL FIELDS IN KAFKA MESSAGES ({len(self.all_fields)} fields)")
        print(f"{'='*80}")
        
        sorted_fields = sorted(self.all_fields)
        for i, field in enumerate(sorted_fields, 1):
            stats = self.field_stats[field]
            non_null_pct = (stats['non_null'] / stats['count'] * 100) if stats['count'] > 0 else 0
            print(f"{i:2d}. {field:40s} | Non-null: {non_null_pct:5.1f}% ({stats['non_null']:,}/{stats['count']:,})")
        
        # === FIELD DETAILS ===
        print(f"\n{'='*80}")
        print(f"🔍 FIELD DETAILS & SAMPLE VALUES")
        print(f"{'='*80}\n")
        
        for field in sorted_fields:
            stats = self.field_stats[field]
            print(f"📌 {field}")
            print(f"   Count:      {stats['count']:,}")
            print(f"   Non-null:   {stats['non_null']:,} ({stats['non_null']/stats['count']*100:.1f}%)")
            print(f"   Null/Empty: {stats['null']:,} ({stats['null']/stats['count']*100:.1f}%)")
            
            if stats['sample_values']:
                print(f"   Samples:    {stats['sample_values'][:3]}")
            
            if len(stats['unique_values']) <= 20 and len(stats['unique_values']) > 0:
                print(f"   Unique:     {sorted(list(stats['unique_values']))[:10]}")
            elif len(stats['unique_values']) > 20:
                print(f"   Unique:     {len(stats['unique_values'])} unique values")
            
            print()
        
        # === TRANSACTION TYPES ===
        print(f"{'='*80}")
        print(f"💳 TRANSACTION TYPES DISTRIBUTION")
        print(f"{'='*80}")
        
        for txn_type, count in self.stats['transaction_types'].most_common():
            pct = count / self.stats['total_messages'] * 100
            print(f"{txn_type:50s} | {count:6,} ({pct:5.1f}%)")
        
        # === CHANNELS ===
        print(f"\n{'='*80}")
        print(f"📡 CHANNEL DISTRIBUTION")
        print(f"{'='*80}")
        
        for channel, count in self.stats['channels'].most_common():
            pct = count / self.stats['total_messages'] * 100
            print(f"{channel:50s} | {count:6,} ({pct:5.1f}%)")
        
        # === STATUSES ===
        print(f"\n{'='*80}")
        print(f"📊 STATUS DISTRIBUTION")
        print(f"{'='*80}")
        
        for status, count in self.stats['statuses'].most_common():
            pct = count / self.stats['total_messages'] * 100
            print(f"{status:50s} | {count:6,} ({pct:5.1f}%)")
        
        # === AMOUNTS ===
        if self.stats['amounts_range']['min'] != float('inf'):
            print(f"\n{'='*80}")
            print(f"💰 TRANSACTION AMOUNTS")
            print(f"{'='*80}")
            print(f"Min Amount:  KES {self.stats['amounts_range']['min']:,.2f}")
            print(f"Max Amount:  KES {self.stats['amounts_range']['max']:,.2f}")
        
        # === CUSTOMERS, DEVICES, ACCOUNTS ===
        print(f"\n{'='*80}")
        print(f"👥 UNIQUE ENTITIES")
        print(f"{'='*80}")
        print(f"Unique Customers:    {len(self.stats['customers']):,}")
        print(f"Unique Devices:      {len(self.stats['devices']):,}")
        print(f"Unique Accounts:     {len(self.stats['accounts']):,}")
        
        # === SAMPLE MESSAGES ===
        print(f"\n{'='*80}")
        print(f"📝 SAMPLE MESSAGES")
        print(f"{'='*80}\n")
        
        samples = min(3, len(self.messages))
        for i in range(samples):
            print(f"Sample {i+1}:")
            print(json.dumps(self.messages[i], indent=2))
            print(f"\n{'-'*80}\n")
        
        # === SUMMARY ===
        print(f"{'='*80}")
        print(f"✅ ANALYSIS SUMMARY")
        print(f"{'='*80}")
        print(f"Total Fields:        {len(self.all_fields)}")
        print(f"Transaction Types:   {len(self.stats['transaction_types'])}")
        print(f"Channels:            {len(self.stats['channels'])}")
        print(f"Statuses:            {len(self.stats['statuses'])}")
        print(f"Unique Customers:    {len(self.stats['customers']):,}")
        print(f"{'='*80}\n")
        
        # === EXPORT OPTIONS ===
        print(f"{'='*80}")
        print(f"💾 EXPORT OPTIONS")
        print(f"{'='*80}")
        print(f"Would you like to export this data?")
        print(f"1. Export field analysis to CSV")
        print(f"2. Export all messages to JSON")
        print(f"3. Export summary statistics to TXT")
        print(f"4. Export all of the above")
        print(f"{'='*80}\n")
    
    def export_field_analysis(self, filename='kafka_field_analysis.csv'):
        """Export field analysis to CSV"""
        import csv
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Field', 'Total_Count', 'Non_Null', 'Null', 
                'Non_Null_Pct', 'Unique_Values', 'Sample_Values'
            ])
            
            for field in sorted(self.all_fields):
                stats = self.field_stats[field]
                non_null_pct = (stats['non_null'] / stats['count'] * 100) if stats['count'] > 0 else 0
                unique_count = len(stats['unique_values']) if len(stats['unique_values']) <= 100 else '100+'
                
                writer.writerow([
                    field,
                    stats['count'],
                    stats['non_null'],
                    stats['null'],
                    f"{non_null_pct:.1f}%",
                    unique_count,
                    ', '.join(str(v) for v in stats['sample_values'][:3])
                ])
        
        print(f"✅ Field analysis exported to: {filename}")
    
    def export_messages(self, filename='kafka_all_messages.json'):
        """Export all messages to JSON"""
        with open(filename, 'w') as f:
            json.dump(self.messages, f, indent=2)
        
        print(f"✅ All messages exported to: {filename}")
    
    def export_summary(self, filename='kafka_summary.txt'):
        """Export summary to text file"""
        with open(filename, 'w') as f:
            f.write(f"Kafka Topic Analysis Summary\n")
            f.write(f"{'='*80}\n\n")
            f.write(f"Topic: {self.kafka_topic}\n")
            f.write(f"Server: {self.kafka_bootstrap_servers}\n")
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total Messages: {self.stats['total_messages']:,}\n")
            f.write(f"Total Fields: {len(self.all_fields)}\n\n")
            
            f.write(f"Transaction Types:\n")
            for txn_type, count in self.stats['transaction_types'].most_common():
                pct = count / self.stats['total_messages'] * 100
                f.write(f"  {txn_type}: {count:,} ({pct:.1f}%)\n")
            
            f.write(f"\nChannels:\n")
            for channel, count in self.stats['channels'].most_common():
                pct = count / self.stats['total_messages'] * 100
                f.write(f"  {channel}: {count:,} ({pct:.1f}%)\n")
        
        print(f"✅ Summary exported to: {filename}")


def main():
    """Main entry point"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze Kafka topic data')
    parser.add_argument('--server', default='172.16.20.76:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='YEA_Transaction_Monitoring', help='Kafka topic')
    parser.add_argument('--max', type=int, default=None, help='Max messages to analyze (default: all)')
    parser.add_argument('--export', action='store_true', help='Export results to files')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("📊 KAFKA TOPIC DATA ANALYZER")
    print("="*80)
    print(f"\n⚙️  Configuration:")
    print(f"   Server:       {args.server}")
    print(f"   Topic:        {args.topic}")
    print(f"   Max messages: {args.max if args.max else 'ALL (may take time!)'}")
    print("\n" + "="*80)
    
    response = input("\n✅ Start analysis? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    analyzer = KafkaDataAnalyzer(
        kafka_bootstrap_servers=args.server,
        kafka_topic=args.topic,
        max_messages=args.max
    )
    
    analyzer.analyze_topic()
    
    # Export if requested
    if args.export or input("\n💾 Export results? (y/n): ").lower() == 'y':
        analyzer.export_field_analysis()
        analyzer.export_messages()
        analyzer.export_summary()


if __name__ == "__main__":
    main()