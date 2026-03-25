from pyspark.sql import functions as F

def validate_transactions(df) -> dict:  
    pii_unmasked_count = df.filter(F.col("is_pii_masked") == False).count()
    null_transaction_id_count = df.filter(F.col("transaction_id").isNull()).count()
    total = df.count()
    distinct = df.select("transaction_id").distinct().count()
    invalid_amount_count = df.filter((F.col("amount_thb") < 1.0) | (F.col("amount_thb") > 999999.0)).count()
    invalid_category_count = df.filter(~F.col("merchant_category").isin(["retail", "food", "transport", "utility", "healthcare", "education"])).count()
    null_lat_count = df.filter(F.col("location_lat").isNull()).count()

    critical_failures = []
    soft_failures = []
    
    if null_transaction_id_count > 0:
        critical_failures.append(f"{null_transaction_id_count} records have null transaction_id")
    if total!= distinct:
        critical_failures.append(f"{total - distinct} records have duplicate transaction_id")
    if pii_unmasked_count > 0:                                                                                                                                             
        critical_failures.append(f"{pii_unmasked_count} records have is_pii_masked = False")
    if invalid_amount_count > 0:
        soft_failures.append(f"{invalid_amount_count} records have amount outside 1.0-999999.0")
    if invalid_category_count > 0:
        soft_failures.append(f"{invalid_category_count} records have invalid merchant_category")
    if total > 0 and null_lat_count / total > 0.2:
        soft_failures.append(f"{null_lat_count} records have null location latitude")
    return {        
        "passed": len(critical_failures) == 0,                                                                                                                             
        "critical_failures": critical_failures,
        "soft_failures": soft_failures
    }
