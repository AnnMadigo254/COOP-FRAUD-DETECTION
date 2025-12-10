"""
Fraud Detection Model Training - Kafka-Only Version
Trains K-means clustering model on YEA transaction data extracted from Kafka.

Model specifications:
- Algorithm: K-means clustering
- Features: 37 (from Kafka messages only)
- Method: Unsupervised outlier detection
- No Customer API dependency

Performance target: 90-92% accuracy
"""

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, confusion_matrix
from itertools import product


def plot_confusion_matrix(cm, classes=['Not Fraud', 'Fraud'], 
                          title='Fraud Detection Confusion Matrix'):
    """Generate and save confusion matrix visualization"""
    plt.figure(figsize=(8, 6))
    plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    thresh = cm.max() / 2.
    for i, j in product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], 'd'),
                horizontalalignment="center",
                color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.savefig('confusion_matrix_kafka_only.png', dpi=300, bbox_inches='tight')
    print("   Confusion matrix saved: confusion_matrix_kafka_only.png")


def train_kafka_only_model(
    data_file='yea_kafka_only_data.csv',
    n_clusters=3,
    test_size=0.3,
    output_dir='pickle'
):
    """
    Train fraud detection model on Kafka-only data
    
    Args:
        data_file: Path to CSV file with extracted Kafka data
        n_clusters: Number of clusters for K-means
        test_size: Proportion of data reserved for testing
        output_dir: Directory to save model artifacts
        
    Returns:
        Tuple of (model, scaler, config)
    """
    
    print("")
    print("="*70)
    print("FRAUD DETECTION MODEL TRAINING - KAFKA-ONLY")
    print("="*70)
    
    # Load data
    print("")
    print(f"Loading data from: {data_file}")
    df = pd.read_csv(data_file)
    
    print(f"   Shape: {df.shape}")
    print(f"   Total samples: {len(df):,}")
    
    # Prepare features
    print("")
    print("Preparing features...")
    
    # Identify feature columns (exclude metadata and target)
    metadata_cols = ['fraud', 'transaction_id', 'customer_id', 'transaction_type', 
                     'transaction_date', 'reference']
    feature_cols = [col for col in df.columns if col not in metadata_cols]
    
    print(f"   Feature columns: {len(feature_cols)}")
    print(f"   Sample features: {', '.join(feature_cols[:5])}")
    
    # Handle missing values
    missing = df[feature_cols].isnull().sum().sum()
    if missing > 0:
        print(f"   Missing values found: {missing}")
        print(f"   Filling missing values with 0")
        df[feature_cols] = df[feature_cols].fillna(0)
    
    X = df[feature_cols].values.astype(float)
    labels = df['fraud'].values
    
    print(f"   Features shape: {X.shape}")
    
    # Check fraud distribution
    print("")
    print("Fraud label distribution:")
    fraud_counts = labels.sum()
    print(f"   Normal transactions: {len(labels) - fraud_counts:,}")
    print(f"   Fraud transactions: {fraud_counts:,}")
    
    fraud_rate = (fraud_counts / len(labels)) * 100
    print(f"   Fraud rate: {fraud_rate:.2f}%")
    
    if fraud_rate == 0:
        print("")
        print("Note: Training in unsupervised mode (outlier detection)")
        print("      No fraud labels available - model will detect anomalies")
    
    # Scale features
    print("")
    print("Scaling features to [0, 1] range...")
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)
    
    print(f"   Scaled range: [{X_scaled.min():.4f}, {X_scaled.max():.4f}]")
    
    # Elbow method for optimal clusters
    print("")
    print("Determining optimal number of clusters...")
    
    clustno = range(1, 10)
    kmeans_list = [MiniBatchKMeans(n_clusters=i, random_state=42) for i in clustno]
    scores = [kmeans_list[i].fit(X_scaled).score(X_scaled) for i in range(len(kmeans_list))]
    
    # Plot elbow curve
    plt.figure(figsize=(10, 6))
    plt.plot(clustno, scores, 'bo-', linewidth=2)
    plt.xlabel('Number of Clusters', fontsize=12)
    plt.ylabel('Within-Cluster Sum of Squares', fontsize=12)
    plt.title('Elbow Method for Optimal K', fontsize=14)
    plt.axvline(x=n_clusters, color='r', linestyle='--', linewidth=2, 
                label=f'Selected: {n_clusters} clusters')
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.savefig('elbow_curve_kafka_only.png', dpi=300, bbox_inches='tight')
    print("   Elbow curve saved: elbow_curve_kafka_only.png")
    
    # Split data
    print("")
    print(f"Splitting data (train: {int((1-test_size)*100)}%, test: {int(test_size*100)}%)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, labels, test_size=test_size, random_state=42, stratify=None
    )
    
    print(f"   Training set: {X_train.shape[0]:,} samples")
    print(f"   Test set: {X_test.shape[0]:,} samples")
    
    # Train model
    print("")
    print("Training K-means clustering model...")
    print(f"   Number of clusters: {n_clusters}")
    print(f"   Algorithm: K-means")
    print(f"   Random state: 42")
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    kmeans.fit(X_train)
    
    print(f"   Model trained successfully")
    print(f"   Iterations to convergence: {kmeans.n_iter_}")
    
    # Make predictions
    print("")
    print("Generating predictions on test set...")
    
    X_test_clusters = kmeans.predict(X_test)
    X_test_clusters_centers = kmeans.cluster_centers_
    
    # Calculate distances from cluster centers
    dist = [
        np.linalg.norm(x - y)
        for x, y in zip(X_test, X_test_clusters_centers[X_test_clusters])
    ]
    
    # Define threshold using percentile method
    threshold_percentile = 95
    threshold = np.percentile(dist, threshold_percentile)
    
    # Create fraud predictions based on distance threshold
    km_y_pred = np.zeros(len(dist))
    km_y_pred[np.array(dist) >= threshold] = 1
    
    print(f"   Distance threshold (95th percentile): {threshold:.4f}")
    print(f"   Transactions flagged as fraud: {km_y_pred.sum():,} / {len(km_y_pred):,} ({km_y_pred.sum()/len(km_y_pred)*100:.1f}%)")
    
    # Evaluate model performance
    print("")
    print("="*70)
    print("MODEL EVALUATION RESULTS")
    print("="*70)
    
    # Confusion matrix
    cm = confusion_matrix(y_test, km_y_pred)
    print("")
    print("Confusion Matrix:")
    print(cm)
    
    # Calculate metrics
    tn, fp, fn, tp = cm.ravel()
    
    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    print("")
    print("Performance Metrics:")
    print(f"   Accuracy: {accuracy:.4f}")
    print(f"   Precision: {precision:.4f}")
    print(f"   Recall: {recall:.4f}")
    print(f"   F1-Score: {f1:.4f}")
    
    # ROC-AUC (if fraud labels available)
    try:
        roc_auc = roc_auc_score(y_test, km_y_pred)
        print(f"   ROC-AUC: {roc_auc:.4f}")
    except:
        print(f"   ROC-AUC: N/A (requires labeled fraud data)")
        roc_auc = 0.0
    
    # Generate confusion matrix plot
    plot_confusion_matrix(cm)
    
    # Save model artifacts
    print("")
    print("Saving model artifacts...")
    
    Path(output_dir).mkdir(exist_ok=True)
    
    # Save trained model
    model_path = f'{output_dir}/fraud_detector_production.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump(kmeans, f)
    print(f"   Model saved: {model_path}")
    
    # Save scaler
    scaler_path = f'{output_dir}/scaler_production.pkl'
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    print(f"   Scaler saved: {scaler_path}")
    
    # Save configuration
    config = {
        'feature_columns': feature_cols,
        'n_clusters': n_clusters,
        'optimal_threshold': threshold_percentile,
        'model_version': '5.0_kafka_only',
        'trained_date': datetime.now().isoformat(),
        'data_source': 'kafka_only',
        'data_file': data_file,
        'n_samples': len(df),
        'n_features': len(feature_cols),
        'fraud_rate': fraud_rate,
        'roc_auc': roc_auc,
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1
    }
    
    config_path = f'{output_dir}/feature_config_production.pkl'
    with open(config_path, 'wb') as f:
        pickle.dump(config, f)
    print(f"   Configuration saved: {config_path}")
    
    # Print summary
    print("")
    print("="*70)
    print("TRAINING COMPLETE")
    print("="*70)
    print("")
    print("Model Summary:")
    print(f"   Data source: Kafka-only (no Customer API)")
    print(f"   Training date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Total samples: {len(df):,}")
    print(f"   Feature count: {len(feature_cols)}")
    print(f"   Cluster count: {n_clusters}")
    print(f"   Fraud rate: {fraud_rate:.2f}%")
    print(f"   Model accuracy: {accuracy:.4f}")
    print(f"   Model precision: {precision:.4f}")
    print(f"   Model recall: {recall:.4f}")
    print(f"   F1-Score: {f1:.4f}")
    
    print("")
    print("Output Files:")
    print(f"   Model: {output_dir}/fraud_detector_production.pkl")
    print(f"   Scaler: {output_dir}/scaler_production.pkl")
    print(f"   Config: {output_dir}/feature_config_production.pkl")
    print(f"   Elbow curve: elbow_curve_kafka_only.png")
    print(f"   Confusion matrix: confusion_matrix_kafka_only.png")
    
    print("")
    print("Next Steps:")
    print("1. Copy pickle files to Django project:")
    print(f"   cp {output_dir}/*.pkl <django_project>/fraud/pickle/")
    print("2. Update Django views.py with Kafka-only version")
    print("3. Restart Django API server")
    print("4. Deploy Kafka consumer")
    print("5. Monitor fraud detection performance")
    
    return kmeans, scaler, config


def main():
    """Main entry point for model training"""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Train fraud detection model on Kafka-only data'
    )
    parser.add_argument('--data', default='yea_kafka_only_data.csv', 
                       help='Input CSV file with training data')
    parser.add_argument('--clusters', type=int, default=3, 
                       help='Number of clusters for K-means')
    parser.add_argument('--test-size', type=float, default=0.3, 
                       help='Test set proportion (0.0 to 1.0)')
    parser.add_argument('--output', default='pickle', 
                       help='Output directory for model artifacts')
    
    args = parser.parse_args()
    
    print("")
    print("="*70)
    print("FRAUD DETECTION MODEL TRAINING - KAFKA-ONLY")
    print("="*70)
    print("")
    print("Configuration:")
    print(f"   Data file: {args.data}")
    print(f"   Clusters: {args.clusters}")
    print(f"   Test size: {args.test_size}")
    print(f"   Output directory: {args.output}")
    print("")
    print("Model Type: Kafka-Only")
    print("   - No Customer API dependency")
    print("   - 37 features from Kafka messages")
    print("   - Unsupervised learning (K-means clustering)")
    print("")
    print("="*70)
    
    # Verify data file exists
    if not Path(args.data).exists():
        print("")
        print(f"ERROR: Data file not found: {args.data}")
        print("")
        print("Please run data extraction first:")
        print("   python extract_kafka_only_data.py")
        return
    
    response = input("\nProceed with training? (y/n): ")
    if response.lower() != 'y':
        print("Training cancelled.")
        return
    
    # Train model
    train_kafka_only_model(
        data_file=args.data,
        n_clusters=args.clusters,
        test_size=args.test_size,
        output_dir=args.output
    )


if __name__ == "__main__":
    main()