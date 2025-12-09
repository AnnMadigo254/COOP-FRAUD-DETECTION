# import datetime
# from datetime import datetime

# from django.shortcuts import render
# import os
# import pickle
# import numpy as np
# import pandas as pd
# from sklearn import datasets
# from django.conf import settings
# from rest_framework import views
# from rest_framework import status
# from rest_framework.response import Response
# from rest_framework.views import APIView

# # Create your views here.

# from django.http import JsonResponse
# from rest_framework.decorators import api_view
# import pickle


# class PredictAPIView(APIView):
#     def post(self, request):
#         try:
#             age = request.data.get("Age")
#             gender = request.data.get("Gender")
#             amount = request.data.get("Amount")
#             merchant_category = request.data.get("Gender")


#         # Load the machine learning model from the pickle file
#             with open('pickle/fraud_detector.pkl', 'rb') as model_file:
#                 model = pickle.load(model_file)

#             # Get the input data from the request
#                 input_data = request.data

#                 required_features = ["age", "amount", "M", "es_barsandrestaurants", "es_contents", 
#                                      "es_fashion", "es_food", "es_health", "es_home", "es_hotelservices", "es_hyper", "es_leisure", "es_otherservices", "es_sportsandtoys", "es_tech", "es_transportation", "es_travel"]

#                 for feature in required_features:
#                     if feature not in input_data:
#                         raise ValueError(f"Missing '{feature}' in input data.")

#             # Prepare the input data as a list for prediction
#                 input_values = [input_data[feature] for feature in required_features]

#                 numeric_fields = ["age", "amount"]
#                 for field in numeric_fields:
#                     if field in input_data:
#                         input_data[field] = float(input_data[field])


#         # Perform prediction using the loaded model[]
#                 print ("input_values", input_values)
#                 prediction = model.predict([input_values])
                
#         # Return the prediction as JSON response
#                 return JsonResponse({'prediction': prediction.tolist()})
#         except Exception as e:
#             return JsonResponse({'error': str(e)}, status=400)


# class PredictRanksAPIView(APIView):
#     def post(self, request):
#         try:
#             # Get the input data from the request
#             #age = request.data.get("Age")
#             gender = request.data.get("Gender")
#             amount = float(request.data.get("Amount")) 
#             merchant_category = request.data.get("MerchantCategory")

#             # AGE
#             # Get the account creation date (assuming it's provided in the request)
#             account_created_at_str = "10/11/2023" # You should adjust this to match the actual key in your request
#             account_created_at = datetime.strptime(account_created_at_str, "%m/%d/%Y")


#             # Calculate the time difference between the current date and account creation date
#             # current_date = datetime.datetime.today()
#             current_date = datetime.now()
#             account_age = current_date - account_created_at


#             # Check if the account age is less than 1 day
#             # is_new_account = account_age.total_seconds() < 24 * 60 * 60  # 24 hours in seconds
#             is_new_account = int(account_age.total_seconds() < 24 * 60 * 60)

#             # GENDER
#             is_male = 1 if gender == "M" else 0
            
#             # AMOUNT 
#               # Check if the amount is more than 1,000,000p
#             is_large_transaction = int(amount > 1000000)

#             # CATEGORIES
#             # Include the merchant category flags as input features
#             #listen to category, if presentoverride default
#             merchant_category = [
#                 request.data.get("es_barsandrestaurants"),
#                 request.data.get("es_contents", 0),
#                 request.data.get("es_fashion", 0),
#                 request.data.get("es_food", 0),
#                 request.data.get("es_health", 0),
#                 request.data.get("es_home", 0),
#                 request.data.get("es_hotelservices", 1),
#                 request.data.get("es_hyper", 0),
#                 request.data.get("es_leisure", 0),
#                 request.data.get("es_otherservices", 0),
#                 request.data.get("es_sportsandtoys", 0),
#                 request.data.get("es_tech", 0),
#                 request.data.get("es_transportation", 0),
#                 request.data.get("es_travel", 0)
#             ]

#             # Prepare the input features for the machine learning model
#             # input_values = [age, amount, is_male]

#             input_values = [
#                 is_male, is_new_account, is_large_transaction
#             ] + merchant_category

#             # Load the machine learning model from the pickle file
#             with open('pickle/fraud_detector.pkl', 'rb') as model_file:
#                 model = pickle.load(model_file)


#         # Perform prediction using the loaded model[]
#             print ("input_values", input_values)
#             prediction = model.predict([input_values])


#         # Return the prediction as JSON response
#             return JsonResponse({'prediction': prediction.tolist()})
        
#         except Exception as e:

#             return JsonResponse({'error': str(e)}, status=400)



"""
Production Fraud Detection API for Co-op Bank YEA
Uses trained production model with real Kafka structure
"""

import pickle
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple

from django.http import JsonResponse
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

import logging

logger = logging.getLogger(__name__)


class FraudDetectionService:
    """Production fraud detection service"""
    
    # Real channelId values from Kafka
    CHANNEL_IDS = [
        '2E08D0409BE26BA3E064020820E70A68',
        '2E08D0409BE76BA3E064020820E70A68',
        '2E08D0409BE16BA3E064020820E70A68',
        '2E08D0409BE46BA3E064020820E70A68'
    ]
    
    # Real transaction types from Kafka
    TRANSACTION_TYPES = [
        'LIMITS_VALIDATION',
        'GET_ACCOUNT_BALANCE',
        'BALANCEENQUIRY',
        'PAY_TO_COOPTILL',
        'POST_DIRECT_PAYMENTS',
        'PERSONAL_ELOAN'
    ]
    
    def __init__(self, model_path: str, scaler_path: str, config_path: str):
        """Initialize fraud detection service"""
        self.model = self._load_pickle(model_path)
        self.scaler = self._load_pickle(scaler_path)
        self.config = self._load_pickle(config_path)
        
        self.feature_columns = self.config['feature_columns']
        self.threshold_percentile = self.config['optimal_threshold']
        
        logger.info(f"Fraud detection loaded: {len(self.feature_columns)} features")
    
    def _load_pickle(self, file_path: str):
        """Load pickled object"""
        try:
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def calculate_age_from_dob(self, date_of_birth: str) -> int:
        """
        Calculate age in years from date of birth
        
        Args:
            date_of_birth: Date in format "YYYY-MM-DD" or "1991-10-31T00:00:00.000"
            
        Returns:
            Age in years
        """
        try:
            # Handle different date formats
            if 'T' in date_of_birth:
                date_of_birth = date_of_birth.split('T')[0]
            
            dob = datetime.strptime(date_of_birth, '%Y-%m-%d')
            today = datetime.now()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            return age
        except Exception as e:
            logger.warning(f"Error calculating age: {str(e)}, defaulting to 30")
            return 30
    
    def calculate_account_age(self, relationship_opening_date: str) -> Tuple[int, int]:
        """
        Calculate account age in days
        
        Args:
            relationship_opening_date: Date in format "YYYY-MM-DD" or with timestamp
            
        Returns:
            Tuple of (account_age_days, is_new_account)
        """
        try:
            if 'T' in relationship_opening_date:
                relationship_opening_date = relationship_opening_date.split('T')[0]
            
            opening_date = datetime.strptime(relationship_opening_date, '%Y-%m-%d')
            today = datetime.now()
            days = (today - opening_date).days
            is_new = 1 if days < 90 else 0
            
            return days, is_new
        except Exception as e:
            logger.warning(f"Error calculating account age: {str(e)}, defaulting to 365 days")
            return 365, 0
    
    def extract_time_features(self, transaction_date: str) -> Dict[str, int]:
        """
        Extract time-based features from transaction date
        
        Args:
            transaction_date: ISO format datetime string
            
        Returns:
            Dict with hour_of_day, day_of_week, is_weekend, is_night
        """
        try:
            dt = datetime.fromisoformat(transaction_date.replace('Z', '+00:00'))
            
            hour = dt.hour
            day_of_week = dt.weekday()  # 0=Monday, 6=Sunday
            is_weekend = 1 if day_of_week >= 5 else 0
            is_night = 1 if (hour >= 22 or hour <= 5) else 0
            
            return {
                'hour_of_day': hour,
                'day_of_week': day_of_week,
                'is_weekend': is_weekend,
                'is_night': is_night
            }
        except Exception as e:
            logger.warning(f"Error extracting time features: {str(e)}, using defaults")
            return {
                'hour_of_day': 12,
                'day_of_week': 3,
                'is_weekend': 0,
                'is_night': 0
            }
    
    def prepare_features(self, transaction_data: Dict[str, Any]) -> np.ndarray:
        """
        Prepare features for prediction
        
        Args:
            transaction_data: Dict with all required fields
            
        Returns:
            Scaled feature array
        """
        # Extract basic info
        age_years = self.calculate_age_from_dob(transaction_data['date_of_birth'])
        gender_encoded = 1 if transaction_data.get('gender', 'F').upper() == 'M' else 0
        amount = float(transaction_data.get('transaction_amount', 0))
        
        # Time features
        time_features = self.extract_time_features(
            transaction_data.get('transaction_date', datetime.now().isoformat())
        )
        
        # Account features
        account_age_days, is_new_account = self.calculate_account_age(
            transaction_data.get('relationship_opening_date', '2020-01-01')
        )
        
        # Risk rating: LOW=0, MEDIUM=1, HIGH=2
        risk_map = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}
        risk_rating = risk_map.get(transaction_data.get('risk_rating', 'LOW'), 0)
        
        # Customer status
        is_active = 1 if transaction_data.get('customer_status') == 'ACTVE' else 0
        
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
        
        # Transaction type one-hot encoding
        txn_type = transaction_data.get('transaction_type', 'PAY_TO_COOPTILL')
        for tt in self.TRANSACTION_TYPES:
            col_name = f'txn_{tt}'
            features[col_name] = 1 if txn_type == tt else 0
        
        # Channel one-hot encoding
        channel_id = transaction_data.get('channelId', self.CHANNEL_IDS[0])
        for ch in self.CHANNEL_IDS:
            col_name = f'ch_{ch}'
            features[col_name] = 1 if channel_id == ch else 0
        
        # Convert to array in correct order
        feature_array = np.array([
            features.get(col, 0) for col in self.feature_columns
        ]).reshape(1, -1).astype(float)
        
        # Scale
        scaled = self.scaler.transform(feature_array)
        
        return scaled
    
    def predict(self, transaction_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict fraud for a transaction
        
        Args:
            transaction_data: Transaction details
            
        Returns:
            Prediction results
        """
        try:
            # Prepare features
            X_scaled = self.prepare_features(transaction_data)
            
            # Predict cluster
            cluster_id = self.model.predict(X_scaled)[0]
            
            # Calculate distance from cluster center
            cluster_center = self.model.cluster_centers_[cluster_id]
            distance = np.linalg.norm(X_scaled[0] - cluster_center)
            
            # Determine fraud based on distance threshold
            # Use the actual threshold from training
            fraud_score = float(distance)
            
            # Calculate percentile of this distance
            # In production, you'd store reference distances from training
            # For now, use a dynamic threshold
            threshold_distance = 0.5  # This should come from training
            is_fraud = int(distance > threshold_distance)
            
            # Risk level
            if distance < 0.3:
                risk_level = "LOW"
            elif distance < 0.5:
                risk_level = "MEDIUM"
            elif distance < 0.7:
                risk_level = "HIGH"
            else:
                risk_level = "CRITICAL"
            
            return {
                'fraud_prediction': is_fraud,
                'fraud_score': round(fraud_score, 4),
                'risk_level': risk_level,
                'cluster_id': int(cluster_id),
                'distance': round(distance, 4)
            }
            
        except Exception as e:
            logger.error(f"Prediction error: {str(e)}")
            raise


# Initialize service
BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_DIR = BASE_DIR / 'pickle'

fraud_detector = FraudDetectionService(
    model_path=str(MODEL_DIR / 'fraud_detector_production.pkl'),
    scaler_path=str(MODEL_DIR / 'scaler_production.pkl'),
    config_path=str(MODEL_DIR / 'feature_config_production.pkl')
)


class FraudPredictionAPIView(APIView):
    """
    API endpoint for fraud prediction
    
    POST /fraud/predict
    """
    
    def post(self, request):
        start_time = datetime.now()
        
        try:
            # Required fields
            required = ['customer_id', 'date_of_birth', 'gender', 'transaction_amount']
            missing = [f for f in required if not request.data.get(f)]
            
            if missing:
                return Response({
                    'error': 'Missing required fields',
                    'missing_fields': missing
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Make prediction
            result = fraud_detector.predict(request.data)
            
            # Determine recommendation
            if result['fraud_prediction'] == 1:
                recommendation = "BLOCK" if result['risk_level'] == "CRITICAL" else "REVIEW"
            else:
                recommendation = "APPROVE"
            
            # Build response
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            response_data = {
                'transaction_id': request.data.get('transaction_id', 'N/A'),
                'customer_id': request.data.get('customer_id'),
                'fraud_prediction': result['fraud_prediction'],
                'fraud_score': result['fraud_score'],
                'risk_level': result['risk_level'],
                'cluster_id': result['cluster_id'],
                'distance': result['distance'],
                'recommendation': recommendation,
                'timestamp': datetime.now().isoformat(),
                'processing_time_ms': round(processing_time, 2)
            }
            
            # Log
            logger.info(
                f"Prediction: TXN={response_data['transaction_id']}, "
                f"Fraud={result['fraud_prediction']}, Risk={result['risk_level']}"
            )
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except ValueError as e:
            return Response({
                'error': 'Validation error',
                'message': str(e)
            }, status=status.HTTP_400_BAD_REQUEST)
            
        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            return Response({
                'error': 'Internal server error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ModelHealthAPIView(APIView):
    """Health check endpoint"""
    
    def get(self, request):
        try:
            config = fraud_detector.config
            
            return Response({
                'status': 'healthy',
                'model_version': config.get('model_version'),
                'trained_date': config.get('trained_date'),
                'features': len(fraud_detector.feature_columns),
                'threshold': fraud_detector.threshold_percentile,
                'performance': {
                    'roc_auc': config.get('roc_auc', 0),
                    'precision': config.get('precision', 0),
                    'recall': config.get('recall', 0)
                },
                'channels': len(FraudDetectionService.CHANNEL_IDS),
                'transaction_types': len(FraudDetectionService.TRANSACTION_TYPES)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            return Response({
                'status': 'unhealthy',
                'error': str(e)
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)