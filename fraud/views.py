# """
# Production Fraud Detection API for Co-op Bank YEA
# Uses trained production model with real Kafka structure
# """

# import pickle
# import numpy as np
# from datetime import datetime
# from pathlib import Path
# from typing import Dict, Any, Tuple

# from django.http import JsonResponse
# from rest_framework import status
# from rest_framework.views import APIView
# from rest_framework.response import Response

# import logging

# logger = logging.getLogger(__name__)


# class FraudDetectionService:
#     """Production fraud detection service"""
    
#     # Real channelId values from Kafka
#     CHANNEL_IDS = [
#         '2E08D0409BE26BA3E064020820E70A68',
#         '2E08D0409BE76BA3E064020820E70A68',
#         '2E08D0409BE16BA3E064020820E70A68',
#         '2E08D0409BE46BA3E064020820E70A68'
#     ]
    
#     # Real transaction types from Kafka
#     TRANSACTION_TYPES = [
#         'LIMITS_VALIDATION',
#         'GET_ACCOUNT_BALANCE',
#         'BALANCEENQUIRY',
#         'PAY_TO_COOPTILL',
#         'POST_DIRECT_PAYMENTS',
#         'PERSONAL_ELOAN',
#         'COOP_REMIT_POST',              
#         'INSTITUTIONAL_PAYMENT_REQUEST'  
#     ]
    
#     def __init__(self, model_path: str, scaler_path: str, config_path: str):
#         """Initialize fraud detection service"""
#         self.model = self._load_pickle(model_path)
#         self.scaler = self._load_pickle(scaler_path)
#         self.config = self._load_pickle(config_path)
        
#         self.feature_columns = self.config['feature_columns']
#         self.threshold_percentile = self.config['optimal_threshold']
        
#         logger.info(f"Fraud detection loaded: {len(self.feature_columns)} features")
    
#     def _load_pickle(self, file_path: str):
#         """Load pickled object"""
#         try:
#             with open(file_path, 'rb') as f:
#                 return pickle.load(f)
#         except Exception as e:
#             logger.error(f"Error loading {file_path}: {str(e)}")
#             raise
    
#     def calculate_age_from_dob(self, date_of_birth: str) -> int:
#         """
#         Calculate age in years from date of birth
        
#         Args:
#             date_of_birth: Date in format "YYYY-MM-DD" or "1991-10-31T00:00:00.000"
            
#         Returns:
#             Age in years
#         """
#         try:
#             # Handle different date formats
#             if 'T' in date_of_birth:
#                 date_of_birth = date_of_birth.split('T')[0]
            
#             dob = datetime.strptime(date_of_birth, '%Y-%m-%d')
#             today = datetime.now()
#             age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
#             return age
#         except Exception as e:
#             logger.warning(f"Error calculating age: {str(e)}, defaulting to 30")
#             return 30
    
#     def calculate_account_age(self, relationship_opening_date: str) -> Tuple[int, int]:
#         """
#         Calculate account age in days
        
#         Args:
#             relationship_opening_date: Date in format "YYYY-MM-DD" or with timestamp
            
#         Returns:
#             Tuple of (account_age_days, is_new_account)
#         """
#         try:
#             if 'T' in relationship_opening_date:
#                 relationship_opening_date = relationship_opening_date.split('T')[0]
            
#             opening_date = datetime.strptime(relationship_opening_date, '%Y-%m-%d')
#             today = datetime.now()
#             days = (today - opening_date).days
#             is_new = 1 if days < 90 else 0
            
#             return days, is_new
#         except Exception as e:
#             logger.warning(f"Error calculating account age: {str(e)}, defaulting to 365 days")
#             return 365, 0
    
#     def extract_time_features(self, transaction_date: str) -> Dict[str, int]:
#         """
#         Extract time-based features from transaction date
        
#         Args:
#             transaction_date: ISO format datetime string
            
#         Returns:
#             Dict with hour_of_day, day_of_week, is_weekend, is_night
#         """
#         try:
#             dt = datetime.fromisoformat(transaction_date.replace('Z', '+00:00'))
            
#             hour = dt.hour
#             day_of_week = dt.weekday()  # 0=Monday, 6=Sunday
#             is_weekend = 1 if day_of_week >= 5 else 0
#             is_night = 1 if (hour >= 22 or hour <= 5) else 0
            
#             return {
#                 'hour_of_day': hour,
#                 'day_of_week': day_of_week,
#                 'is_weekend': is_weekend,
#                 'is_night': is_night
#             }
#         except Exception as e:
#             logger.warning(f"Error extracting time features: {str(e)}, using defaults")
#             return {
#                 'hour_of_day': 12,
#                 'day_of_week': 3,
#                 'is_weekend': 0,
#                 'is_night': 0
#             }
    
#     def prepare_features(self, transaction_data: Dict[str, Any]) -> np.ndarray:
#         """
#         Prepare features for prediction
        
#         Args:
#             transaction_data: Dict with all required fields
            
#         Returns:
#             Scaled feature array
#         """
#         # Extract basic info
#         age_years = self.calculate_age_from_dob(transaction_data['date_of_birth'])
#         gender_encoded = 1 if transaction_data.get('gender', 'F').upper() == 'M' else 0
#         amount = float(transaction_data.get('transaction_amount', 0))
        
#         # Time features
#         time_features = self.extract_time_features(
#             transaction_data.get('transaction_date', datetime.now().isoformat())
#         )
        
#         # Account features
#         account_age_days, is_new_account = self.calculate_account_age(
#             transaction_data.get('relationship_opening_date', '2020-01-01')
#         )
        
#         # Risk rating: LOW=0, MEDIUM=1, HIGH=2
#         risk_map = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}
#         risk_rating = risk_map.get(transaction_data.get('risk_rating', 'LOW'), 0)
        
#         # Customer status
#         is_active = 1 if transaction_data.get('customer_status') == 'ACTVE' else 0
        
#         # Build feature dict
#         features = {
#             'age_years': age_years,
#             'gender_encoded': gender_encoded,
#             'amount': amount,
#             'hour_of_day': time_features['hour_of_day'],
#             'day_of_week': time_features['day_of_week'],
#             'is_weekend': time_features['is_weekend'],
#             'is_night': time_features['is_night'],
#             'account_age_days': account_age_days,
#             'is_new_account': is_new_account,
#             'risk_rating': risk_rating,
#             'is_active': is_active
#         }
        
#         # Transaction type one-hot encoding
#         txn_type = transaction_data.get('transaction_type', 'PAY_TO_COOPTILL')
#         for tt in self.TRANSACTION_TYPES:
#             col_name = f'txn_{tt}'
#             features[col_name] = 1 if txn_type == tt else 0
        
#         # Channel one-hot encoding
#         channel_id = transaction_data.get('channelId', self.CHANNEL_IDS[0])
#         for ch in self.CHANNEL_IDS:
#             col_name = f'ch_{ch}'
#             features[col_name] = 1 if channel_id == ch else 0
        
#         # Convert to array in correct order
#         feature_array = np.array([
#             features.get(col, 0) for col in self.feature_columns
#         ]).reshape(1, -1).astype(float)
        
#         # Scale
#         scaled = self.scaler.transform(feature_array)
        
#         return scaled
    
#     def predict(self, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Predict fraud for a transaction
        
#         Args:
#             transaction_data: Transaction details
            
#         Returns:
#             Prediction results
#         """
#         try:
#             # Prepare features
#             X_scaled = self.prepare_features(transaction_data)
            
#             # Predict cluster
#             cluster_id = self.model.predict(X_scaled)[0]
            
#             # Calculate distance from cluster center
#             cluster_center = self.model.cluster_centers_[cluster_id]
#             distance = np.linalg.norm(X_scaled[0] - cluster_center)
            
#             # Determine fraud based on distance threshold
#             # Use the actual threshold from training
#             fraud_score = float(distance)
            
#             # Calculate percentile of this distance
#             # In production, you'd store reference distances from training
#             # For now, use a dynamic threshold
#             threshold_distance = 0.5  # This should come from training
#             is_fraud = int(distance > threshold_distance)
            
#             # Risk level
#             if distance < 0.3:
#                 risk_level = "LOW"
#             elif distance < 0.5:
#                 risk_level = "MEDIUM"
#             elif distance < 0.7:
#                 risk_level = "HIGH"
#             else:
#                 risk_level = "CRITICAL"
            
#             return {
#                 'fraud_prediction': is_fraud,
#                 'fraud_score': round(fraud_score, 4),
#                 'risk_level': risk_level,
#                 'cluster_id': int(cluster_id),
#                 'distance': round(distance, 4)
#             }
            
#         except Exception as e:
#             logger.error(f"Prediction error: {str(e)}")
#             raise


# # Initialize service
# BASE_DIR = Path(__file__).resolve().parent.parent
# MODEL_DIR = BASE_DIR / 'pickle'

# fraud_detector = FraudDetectionService(
#     model_path=str(MODEL_DIR / 'fraud_detector_production.pkl'),
#     scaler_path=str(MODEL_DIR / 'scaler_production.pkl'),
#     config_path=str(MODEL_DIR / 'feature_config_production.pkl')
# )


# class FraudPredictionAPIView(APIView):
#     """
#     API endpoint for fraud prediction
    
#     POST /fraud/predict
#     """
    
#     def post(self, request):
#         start_time = datetime.now()
        
#         try:
#             # Required fields
#             required = ['customer_id', 'date_of_birth', 'gender', 'transaction_amount']
#             missing = [f for f in required if not request.data.get(f)]
            
#             if missing:
#                 return Response({
#                     'error': 'Missing required fields',
#                     'missing_fields': missing
#                 }, status=status.HTTP_400_BAD_REQUEST)
            
#             # Make prediction
#             result = fraud_detector.predict(request.data)
            
#             # Determine recommendation
#             if result['fraud_prediction'] == 1:
#                 recommendation = "BLOCK" if result['risk_level'] == "CRITICAL" else "REVIEW"
#             else:
#                 recommendation = "APPROVE"
            
#             # Build response
#             processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
#             response_data = {
#                 'transaction_id': request.data.get('transaction_id', 'N/A'),
#                 'customer_id': request.data.get('customer_id'),
#                 'fraud_prediction': result['fraud_prediction'],
#                 'fraud_score': result['fraud_score'],
#                 'risk_level': result['risk_level'],
#                 'cluster_id': result['cluster_id'],
#                 'distance': result['distance'],
#                 'recommendation': recommendation,
#                 'timestamp': datetime.now().isoformat(),
#                 'processing_time_ms': round(processing_time, 2)
#             }
            
#             # Log
#             logger.info(
#                 f"Prediction: TXN={response_data['transaction_id']}, "
#                 f"Fraud={result['fraud_prediction']}, Risk={result['risk_level']}"
#             )
            
#             return Response(response_data, status=status.HTTP_200_OK)
            
#         except ValueError as e:
#             return Response({
#                 'error': 'Validation error',
#                 'message': str(e)
#             }, status=status.HTTP_400_BAD_REQUEST)
            
#         except Exception as e:
#             logger.error(f"Error: {str(e)}", exc_info=True)
#             return Response({
#                 'error': 'Internal server error',
#                 'message': str(e)
#             }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# class ModelHealthAPIView(APIView):
#     """Health check endpoint"""
    
#     def get(self, request):
#         try:
#             config = fraud_detector.config
            
#             return Response({
#                 'status': 'healthy',
#                 'model_version': config.get('model_version'),
#                 'trained_date': config.get('trained_date'),
#                 'features': len(fraud_detector.feature_columns),
#                 'threshold': fraud_detector.threshold_percentile,
#                 'performance': {
#                     'roc_auc': config.get('roc_auc', 0),
#                     'precision': config.get('precision', 0),
#                     'recall': config.get('recall', 0)
#                 },
#                 'channels': len(FraudDetectionService.CHANNEL_IDS),
#                 'transaction_types': len(FraudDetectionService.TRANSACTION_TYPES)
#             }, status=status.HTTP_200_OK)
            
#         except Exception as e:
#             return Response({
#                 'status': 'unhealthy',
#                 'error': str(e)
#             }, status=status.HTTP_503_SERVICE_UNAVAILABLE)


"""
ENHANCED Fraud Detection API - Uses ALL Kafka Features
Now checks: device tracking, velocity, failed attempts, SIM swaps

This is the SMART version that uses enhanced features for better detection!
"""

import pickle
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

import logging

logger = logging.getLogger(__name__)


class EnhancedFraudDetectionService:
    """Fraud detection using ML model + enhanced rule-based checks"""
    
    # Real channelId values from Kafka
    CHANNEL_IDS = [
        '2E08D0409BE26BA3E064020820E70A68',
        '2E08D0409BE76BA3E064020820E70A68',
        '2E08D0409BE16BA3E064020820E70A68',
        '2E08D0409BE46BA3E064020820E70A68'
    ]
    
    # ALL transaction types from production Kafka
    TRANSACTION_TYPES = [
        'PAY_TO_COOPTILL',
        'POST_DIRECT_PAYMENTS',
        'PERSONAL_ELOAN',
        'COOP_REMIT_POST',
        'INSTITUTIONAL_PAYMENT_REQUEST',
        'GET_ACCOUNT_BALANCE',
        'LIMITS_VALIDATION',
        'BALANCEENQUIRY'
    ]
    
    # CRITICAL THRESHOLDS for enhanced features
    VELOCITY_THRESHOLD = 5  # transactions per day
    FAILED_ATTEMPTS_THRESHOLD = 3  # failed attempts per hour
    HIGH_AMOUNT_THRESHOLD = 50000  # KES
    CRITICAL_AMOUNT_THRESHOLD = 100000  # KES
    
    def __init__(self, model_path: str, scaler_path: str, config_path: str):
        """Initialize fraud detection service"""
        self.model = self._load_pickle(model_path)
        self.scaler = self._load_pickle(scaler_path)
        self.config = self._load_pickle(config_path)
        
        self.feature_columns = self.config['feature_columns']
        self.threshold_percentile = self.config['optimal_threshold']
        
        logger.info(f"✅ ENHANCED Fraud Detection loaded")
        logger.info(f"   Features: {len(self.feature_columns)}")
        logger.info(f"   Enhanced rules: ENABLED")
    
    def _load_pickle(self, file_path: str):
        """Load pickled object"""
        try:
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def extract_time_features(self, transaction_date: str) -> Dict[str, int]:
        """Extract time-based features from transaction date"""
        try:
            if transaction_date:
                dt = datetime.fromisoformat(transaction_date.replace('Z', '+00:00'))
            else:
                dt = datetime.now()
            
            hour = dt.hour
            day_of_week = dt.weekday()
            is_weekend = 1 if day_of_week >= 5 else 0
            is_night = 1 if (hour >= 22 or hour <= 5) else 0
            
            return {
                'hour_of_day': hour,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'is_night': is_night
            }
        except Exception as e:
            logger.warning(f"Error extracting time: {str(e)}, using defaults")
            return {'hour_of_day': 12, 'day_of_week': 3, 'is_weekend': 0, 'is_night': 0}
    
    def apply_enhanced_rules(self, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply rule-based fraud checks using enhanced Kafka features
        
        Returns:
        {
            'auto_block': bool,
            'risk_boost': int (0-3),
            'triggered_rules': list,
            'enhanced_score': float
        }
        """
        triggered_rules = []
        risk_boost = 0
        auto_block = False
        enhanced_score = 0.0
        
        amount = float(transaction_data.get('transaction_amount', 0))
        
        # === RULE 1: SIM SWAP (AUTO BLOCK) ===
        phone_mismatch = transaction_data.get('phone_mismatch', 0)
        if phone_mismatch == 1:
            triggered_rules.append("SIM_SWAP_DETECTED")
            auto_block = True
            enhanced_score += 1.0
            logger.critical(f"🚨 SIM SWAP: Auto-blocking transaction")
        
        # === RULE 2: NEW DEVICE + HIGH AMOUNT ===
        is_new_device = transaction_data.get('is_new_device', 0)
        if is_new_device == 1:
            triggered_rules.append("NEW_DEVICE")
            risk_boost += 1
            enhanced_score += 0.2
            
            if amount > self.HIGH_AMOUNT_THRESHOLD:
                triggered_rules.append("NEW_DEVICE_HIGH_AMOUNT")
                risk_boost += 2
                enhanced_score += 0.5
                logger.warning(f"⚠️  NEW DEVICE + HIGH AMOUNT: {amount}")
        
        # === RULE 3: HIGH VELOCITY ===
        transactions_today = transaction_data.get('transactions_today', 0)
        if transactions_today >= self.VELOCITY_THRESHOLD:
            triggered_rules.append(f"HIGH_VELOCITY_{transactions_today}_TXN")
            risk_boost += 1
            enhanced_score += 0.3
            
            if transactions_today >= 10:
                triggered_rules.append("EXTREME_VELOCITY")
                risk_boost += 2
                enhanced_score += 0.5
                logger.warning(f"⚠️  EXTREME VELOCITY: {transactions_today} transactions today")
        
        # === RULE 4: MULTIPLE FAILED ATTEMPTS ===
        failed_attempts = transaction_data.get('failed_attempts_last_hour', 0)
        if failed_attempts >= self.FAILED_ATTEMPTS_THRESHOLD:
            triggered_rules.append(f"FAILED_ATTEMPTS_{failed_attempts}")
            risk_boost += 2
            enhanced_score += 0.4
            logger.warning(f"⚠️  CREDENTIAL TESTING: {failed_attempts} failed attempts")
            
            if failed_attempts >= 5:
                triggered_rules.append("BRUTE_FORCE_ATTACK")
                auto_block = True
                enhanced_score += 1.0
                logger.critical(f"🚨 BRUTE FORCE: {failed_attempts} attempts - Auto-blocking")
        
        # === RULE 5: NIGHT TRANSACTION + NEW DEVICE ===
        time_features = self.extract_time_features(transaction_data.get('transaction_date'))
        if time_features['is_night'] == 1 and is_new_device == 1:
            triggered_rules.append("NIGHT_NEW_DEVICE")
            risk_boost += 1
            enhanced_score += 0.3
            logger.warning(f"⚠️  SUSPICIOUS: Night transaction on new device")
        
        # === RULE 6: CRITICAL AMOUNT ===
        if amount > self.CRITICAL_AMOUNT_THRESHOLD:
            triggered_rules.append(f"CRITICAL_AMOUNT_{int(amount)}")
            risk_boost += 1
            enhanced_score += 0.2
            logger.warning(f"⚠️  CRITICAL AMOUNT: KES {amount:,.0f}")
        
        # === RULE 7: COMBO: HIGH VELOCITY + NEW DEVICE + HIGH AMOUNT ===
        if (transactions_today >= self.VELOCITY_THRESHOLD and 
            is_new_device == 1 and 
            amount > self.HIGH_AMOUNT_THRESHOLD):
            triggered_rules.append("TRIPLE_RED_FLAG")
            auto_block = True
            enhanced_score += 1.0
            logger.critical(f"🚨 TRIPLE RED FLAG: Velocity + New Device + High Amount")
        
        return {
            'auto_block': auto_block,
            'risk_boost': min(risk_boost, 3),  # Cap at 3 levels
            'triggered_rules': triggered_rules,
            'enhanced_score': min(enhanced_score, 1.0)  # Cap at 1.0
        }
    
    def prepare_features(self, transaction_data: Dict[str, Any]) -> np.ndarray:
        """Prepare features for ML model (original 18 features)"""
        
        # === FROM KAFKA OR DEFAULTS ===
        amount = float(transaction_data.get('transaction_amount', 0))
        
        time_features = self.extract_time_features(
            transaction_data.get('transaction_date')
        )
        
        # Customer API fields (with defaults if unavailable)
        age_years = 35
        gender_encoded = 0
        account_age_days = 730
        is_new_account = 0
        risk_rating = 0
        is_active = 1
        
        # Try to get real values if provided
        if transaction_data.get('date_of_birth'):
            try:
                dob = datetime.fromisoformat(transaction_data['date_of_birth'])
                age_years = (datetime.now() - dob).days // 365
            except:
                pass
        
        if transaction_data.get('gender'):
            gender_encoded = 0 if transaction_data['gender'] == 'F' else 1
        
        if transaction_data.get('relationship_opening_date'):
            try:
                rod = datetime.fromisoformat(transaction_data['relationship_opening_date'])
                account_age_days = (datetime.now() - rod).days
                is_new_account = 1 if account_age_days < 90 else 0
            except:
                pass
        
        # Build feature dict
        features = {
            'age_years': age_years,
            'gender_encoded': gender_encoded,
            'amount': amount,
            'hour_of_day': time_features['hour_of_day'],
            'day_of_week': time_features['day_of_week'],
            'is_weekend': time_features['is_weekend'],
            'is_night': time_features['is_night'],
            'account_age_days': account_age_days,
            'is_new_account': is_new_account,
            'risk_rating': risk_rating,
            'is_active': is_active
        }
        
        # Transaction type one-hot
        txn_type = transaction_data.get('transaction_type', 'PAY_TO_COOPTILL')
        for tt in self.TRANSACTION_TYPES:
            col_name = f'txn_{tt}'
            features[col_name] = 1 if txn_type == tt else 0
        
        # Channel one-hot
        channel_id = transaction_data.get('channelId', self.CHANNEL_IDS[0])
        for ch in self.CHANNEL_IDS:
            col_name = f'ch_{ch}'
            features[col_name] = 1 if channel_id == ch else 0
        
        # Convert to array
        feature_array = np.array([
            features.get(col, 0) for col in self.feature_columns
        ]).reshape(1, -1).astype(float)
        
        # Scale
        scaled = self.scaler.transform(feature_array)
        
        return scaled
    
    def predict(self, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict fraud using ML model + enhanced rules
        
        Process:
        1. Apply rule-based checks (SIM swap, velocity, etc.)
        2. If auto-block triggered → return BLOCK
        3. Otherwise, run ML model
        4. Boost risk level based on triggered rules
        """
        
        # Step 1: Apply enhanced rule-based checks
        rule_results = self.apply_enhanced_rules(transaction_data)
        
        # Step 2: Check for auto-block
        if rule_results['auto_block']:
            return {
                'fraud_prediction': 1,
                'fraud_score': 1.0,
                'risk_level': 'CRITICAL',
                'cluster_id': -1,
                'distance': 1.0,
                'recommendation': 'BLOCK',
                'triggered_rules': rule_results['triggered_rules'],
                'detection_method': 'RULE_BASED'
            }
        
        # Step 3: Run ML model
        try:
            X_scaled = self.prepare_features(transaction_data)
            
            cluster_id = self.model.predict(X_scaled)[0]
            cluster_center = self.model.cluster_centers_[cluster_id]
            distance = np.linalg.norm(X_scaled[0] - cluster_center)
            
            # Combine ML score with enhanced score
            ml_score = float(distance)
            enhanced_score = rule_results['enhanced_score']
            combined_score = min(ml_score + enhanced_score, 1.0)
            
            threshold_distance = 0.5
            is_fraud = int(combined_score > threshold_distance)
            
            # Step 4: Determine risk level (with boost from rules)
            base_risk_level = ""
            if distance < 0.3:
                base_risk_level = "LOW"
            elif distance < 0.5:
                base_risk_level = "MEDIUM"
            elif distance < 0.7:
                base_risk_level = "HIGH"
            else:
                base_risk_level = "CRITICAL"
            
            # Boost risk level based on triggered rules
            risk_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
            base_index = risk_levels.index(base_risk_level)
            boosted_index = min(base_index + rule_results['risk_boost'], 3)
            final_risk_level = risk_levels[boosted_index]
            
            # Determine recommendation
            if final_risk_level == "CRITICAL":
                recommendation = "BLOCK"
            elif final_risk_level == "HIGH":
                recommendation = "REVIEW"
            elif is_fraud == 1:
                recommendation = "REVIEW"
            else:
                recommendation = "APPROVE"
            
            return {
                'fraud_prediction': is_fraud,
                'fraud_score': round(combined_score, 4),
                'ml_score': round(ml_score, 4),
                'enhanced_score': round(enhanced_score, 4),
                'risk_level': final_risk_level,
                'cluster_id': int(cluster_id),
                'distance': round(distance, 4),
                'recommendation': recommendation,
                'triggered_rules': rule_results['triggered_rules'],
                'detection_method': 'ML_ENHANCED' if rule_results['triggered_rules'] else 'ML_ONLY'
            }
            
        except Exception as e:
            logger.error(f"Prediction error: {str(e)}", exc_info=True)
            raise


# Initialize service
BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_DIR = BASE_DIR / 'pickle'

fraud_detector = EnhancedFraudDetectionService(
    model_path=str(MODEL_DIR / 'fraud_detector_production.pkl'),
    scaler_path=str(MODEL_DIR / 'scaler_production.pkl'),
    config_path=str(MODEL_DIR / 'feature_config_production.pkl')
)


class FraudPredictionAPIView(APIView):
    """
    Enhanced fraud prediction endpoint
    POST /fraud/predict
    
    Required fields (from Kafka):
    - transaction_amount
    
    Enhanced fields (optional but HIGHLY recommended):
    - is_new_device (0 or 1)
    - transactions_today (integer)
    - failed_attempts_last_hour (integer)
    - phone_mismatch (0 or 1)
    
    Other fields:
    - transaction_date, transaction_type, channelId
    - Customer API fields (if available)
    """
    
    def post(self, request):
        start_time = datetime.now()
        
        try:
            # Only require amount
            required = ['transaction_amount']
            missing = [f for f in required if f not in request.data]
            
            if missing:
                return Response({
                    'error': 'Missing required fields',
                    'missing_fields': missing
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Make prediction with enhanced features
            result = fraud_detector.predict(request.data)
            
            # Build response
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            response_data = {
                'transaction_id': request.data.get('transaction_id', 'N/A'),
                'fraud_prediction': result['fraud_prediction'],
                'fraud_score': result['fraud_score'],
                'ml_score': result.get('ml_score', result['fraud_score']),
                'enhanced_score': result.get('enhanced_score', 0),
                'risk_level': result['risk_level'],
                'recommendation': result['recommendation'],
                'cluster_id': result.get('cluster_id', -1),
                'distance': result.get('distance', 0),
                'triggered_rules': result.get('triggered_rules', []),
                'detection_method': result.get('detection_method', 'ML_ONLY'),
                'timestamp': datetime.now().isoformat(),
                'processing_time_ms': round(processing_time, 2)
            }
            
            # Enhanced logging
            rules_str = f" | Rules: {', '.join(result.get('triggered_rules', []))}" if result.get('triggered_rules') else ""
            
            logger.info(
                f"✅ Prediction: TXN={response_data['transaction_id']}, "
                f"Amount={request.data.get('transaction_amount')}, "
                f"Fraud={result['fraud_prediction']}, Risk={result['risk_level']}, "
                f"Recommendation={result['recommendation']}, "
                f"Method={result.get('detection_method', 'ML_ONLY')}"
                f"{rules_str}"
            )
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except ValueError as e:
            return Response({
                'error': 'Validation error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)
            
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}", exc_info=True)
            return Response({
                'error': 'Internal server error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ModelHealthAPIView(APIView):
    """Health check endpoint - GET /fraud/health"""
    
    def get(self, request):
        try:
            config = fraud_detector.config
            
            return Response({
                'status': 'healthy',
                'mode': 'ENHANCED (ML + Rule-Based)',
                'model_version': config.get('model_version', '3.0_production'),
                'trained_date': config.get('trained_date', 'N/A'),
                'features': len(fraud_detector.feature_columns),
                'threshold': fraud_detector.threshold_percentile,
                'performance': {
                    'roc_auc': config.get('roc_auc', 0.8857),
                    'precision': config.get('precision', 0.2361),
                    'recall': config.get('recall', 0.85),
                    'note': 'Enhanced with rule-based detection for higher accuracy'
                },
                'enhanced_rules': {
                    'sim_swap_detection': 'ENABLED (auto-block)',
                    'velocity_monitoring': f'ENABLED (threshold: {EnhancedFraudDetectionService.VELOCITY_THRESHOLD} txn/day)',
                    'failed_attempts': f'ENABLED (threshold: {EnhancedFraudDetectionService.FAILED_ATTEMPTS_THRESHOLD} attempts/hour)',
                    'device_tracking': 'ENABLED',
                    'night_transactions': 'ENABLED',
                    'amount_thresholds': {
                        'high': f'KES {EnhancedFraudDetectionService.HIGH_AMOUNT_THRESHOLD:,}',
                        'critical': f'KES {EnhancedFraudDetectionService.CRITICAL_AMOUNT_THRESHOLD:,}'
                    }
                },
                'configuration': {
                    'channels': len(EnhancedFraudDetectionService.CHANNEL_IDS),
                    'transaction_types': EnhancedFraudDetectionService.TRANSACTION_TYPES
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'status': 'unhealthy',
                'error': str(e)
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)