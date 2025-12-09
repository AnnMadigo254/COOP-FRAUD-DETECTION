"""
YEA Fraud Detection - Real-time Dashboard
Access: http://server-ip:5000

Team can monitor:
- Total transactions processed
- Fraud detection rate
- Blocked/Reviewed/Approved counts
- Recent transactions in real-time
"""

from flask import Flask, render_template, jsonify
import sqlite3
from datetime import datetime, timedelta
import os

app = Flask(__name__)

DB_FILE = 'fraud_events.db'

def init_db():
    """Initialize database"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS fraud_events
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp TEXT,
                  transaction_id TEXT,
                  customer_id TEXT,
                  amount REAL,
                  transaction_type TEXT,
                  fraud_prediction INTEGER,
                  risk_level TEXT,
                  recommendation TEXT,
                  fraud_score REAL,
                  processing_time_ms REAL)''')
    conn.commit()
    conn.close()
    print("✅ Database initialized")

init_db()

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/stats')
def get_stats():
    """Get statistics for last 24 hours"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    yesterday = (datetime.now() - timedelta(days=1)).isoformat()
    
    # Total processed
    c.execute("SELECT COUNT(*) FROM fraud_events WHERE timestamp > ?", (yesterday,))
    total = c.fetchone()[0]
    
    # Fraud detected
    c.execute("SELECT COUNT(*) FROM fraud_events WHERE fraud_prediction = 1 AND timestamp > ?", (yesterday,))
    fraud_count = c.fetchone()[0]
    
    # By recommendation
    c.execute("""SELECT recommendation, COUNT(*) 
                 FROM fraud_events 
                 WHERE timestamp > ? 
                 GROUP BY recommendation""", (yesterday,))
    recommendations = dict(c.fetchall())
    
    # By risk level
    c.execute("""SELECT risk_level, COUNT(*) 
                 FROM fraud_events 
                 WHERE timestamp > ? 
                 GROUP BY risk_level""", (yesterday,))
    risk_levels = dict(c.fetchall())
    
    # Average processing time
    c.execute("SELECT AVG(processing_time_ms) FROM fraud_events WHERE timestamp > ?", (yesterday,))
    avg_time = c.fetchone()[0] or 0
    
    conn.close()
    
    return jsonify({
        'total_processed': total,
        'fraud_detected': fraud_count,
        'fraud_rate': (fraud_count/total*100) if total > 0 else 0,
        'blocked': recommendations.get('BLOCK', 0),
        'reviewed': recommendations.get('REVIEW', 0),
        'approved': recommendations.get('APPROVE', 0),
        'risk_levels': risk_levels,
        'avg_processing_time': round(avg_time, 2)
    })

@app.route('/api/recent')
def get_recent():
    """Get recent transactions"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute("""SELECT timestamp, transaction_id, customer_id, amount, 
                        transaction_type, fraud_prediction, risk_level, 
                        recommendation, fraud_score, processing_time_ms
                 FROM fraud_events 
                 ORDER BY timestamp DESC 
                 LIMIT 100""")
    
    events = []
    for row in c.fetchall():
        events.append({
            'timestamp': row[0],
            'transaction_id': row[1],
            'customer_id': row[2],
            'amount': row[3],
            'transaction_type': row[4],
            'fraud_prediction': row[5],
            'risk_level': row[6],
            'recommendation': row[7],
            'fraud_score': row[8],
            'processing_time_ms': row[9]
        })
    
    conn.close()
    return jsonify(events)

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'fraud-dashboard',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🛡️  YEA FRAUD DETECTION DASHBOARD")
    print("="*70)
    print(f"\n✅ Starting dashboard on http://0.0.0.0:5000")
    print(f"📊 Team can access at: http://your-server-ip:5000\n")
    
    app.run(host='0.0.0.0', port=5000, debug=False)