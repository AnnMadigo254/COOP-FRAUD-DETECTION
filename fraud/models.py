from django.db import models

class TransactionDetail(models.Model):
    # Define your model fields here
    age = models.CharField(max_length=100)
    amount = models.CharField(max_length=100)
    gender = models.CharField(max_length=100)
    merchant_category = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'{self.field1} - {self.created_at}'
