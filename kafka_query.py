#!/usr/bin/env python3
"""
Find channelId values from Kafka
This is the field we'll use for fraud detection
"""

from kafka import KafkaConsumer
import json
import numpy as np
from collections import Counter

print("\n" + "="*70)
print("FINDING channelId VALUES IN KAFKA")
print("="*70 + "\n")

bootstrap_servers = '172.16.20.76'
topic = 'YEA_Transaction_Monitoring'

print(f"📡 Connecting to Kafka: {bootstrap_servers}")
print(f"📋 Topic: {topic}\n")

try:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print("✅ Connected! Reading messages...\n")
    
    channel_ids = []
    transaction_types = []
    amounts = []
    samples = []
    count = 0
    
    for message in consumer:
        txn = message.value
        
        # Get channelId (try different variations)
        channel_id = (
            txn.get('channelId') or 
            txn.get('channel_id') or 
            txn.get('ChannelId') or 
            txn.get('channel') or
            'UNKNOWN'
        )
        
        channel_ids.append(channel_id)
        transaction_types.append(txn.get('transaction_type', 'N/A'))
        
        amount = txn.get('transaction_amount', 0)
        try:
            amounts.append(float(amount))
        except:
            amounts.append(0.0)
        
        # Collect samples with actual payment amounts
        if len(samples) < 10 and float(amount or 0) > 0:
            samples.append({
                'channelId': channel_id,
                'amount': amount,
                'type': txn.get('transaction_type', 'N/A'),
                'customer': txn.get('customer_id', 'N/A')
            })
        
        count += 1
        if count % 20 == 0:
            print(f"   Read {count} messages...")
        
        if count >= 200:  # Read more messages
            break
    
    consumer.close()
    
    print(f"\n✅ Analyzed {count} messages\n")
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    # Show channelId distribution
    channel_counts = Counter(channel_ids)
    
    print(f"\n📱 Found {len(channel_counts)} unique channelId values:\n")
    print("-"*70)
    print(f"{'CHANNEL_ID':<40} {'COUNT':<10} {'%':<10}")
    print("-"*70)
    
    for channel, cnt in channel_counts.most_common():
        pct = (cnt/len(channel_ids))*100
        print(f"{channel:<40} {cnt:<10} {pct:>6.1f}%")
    
    print("-"*70)
    
    # Show transaction types
    type_counts = Counter(transaction_types)
    print(f"\n📋 Transaction Types:\n")
    print("-"*70)
    for txn_type, cnt in type_counts.most_common():
        pct = (cnt/len(transaction_types))*100
        print(f"{txn_type:<40} {cnt:<10} {pct:>6.1f}%")
    
    # Show amount statistics
    valid_amounts = [a for a in amounts if a > 0]
    if valid_amounts:
        print(f"\n💰 Transaction Amounts (non-zero only):")
        print(f"   Total with amounts: {len(valid_amounts)}")
        print(f"   Mean: KES {np.mean(valid_amounts):,.2f}")
        print(f"   Median: KES {np.median(valid_amounts):,.2f}")
        print(f"   Max: KES {max(valid_amounts):,.2f}")
    else:
        print("\n⚠️  No transactions with amounts > 0 found")
    
    # Show samples
    if samples:
        print(f"\n📝 Sample Transactions with Amounts:")
        print("-"*70)
        for i, s in enumerate(samples, 1):
            print(f"\n{i}. Channel: {s['channelId']}")
            print(f"   Amount: KES {s['amount']}")
            print(f"   Type: {s['type']}")
            print(f"   Customer: {s['customer']}")
    
    # Generate code for model
    print("\n" + "="*70)
    print("✅ USE THESE channelId VALUES IN YOUR MODEL")
    print("="*70)
    
    print("\nChannels found:")
    for channel, _ in channel_counts.most_common():
        print(f"  - {channel}")
    
    print("\n💡 Next step: Train model using these actual channelId values!")
    
    # Save results
    with open('channelId_values.txt', 'w') as f:
        f.write("=== channelId Distribution ===\n\n")
        for channel, cnt in channel_counts.most_common():
            f.write(f"{channel}: {cnt}\n")
        f.write("\n=== Transaction Types ===\n\n")
        for txn_type, cnt in type_counts.most_common():
            f.write(f"{txn_type}: {cnt}\n")
    
    print("\n✅ Results saved to: channelId_values.txt\n")

except Exception as e:
    print(f"\n❌ Error: {str(e)}\n")
    import traceback
    traceback.print_exc()

# Import numpy for statistics
import numpy as np

print("="*70 + "\n")