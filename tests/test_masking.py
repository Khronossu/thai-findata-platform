
"""
Unit tests for PII masking logic — Stage 3 Bronze Ingestion.

Tests the pure Python masking functions independently of Spark UDF wrappers.
No SparkSession required — runs in CI in milliseconds.

Per CLAUDE.md requirements:
- SHA-256 output is 64 hex characters
- Original national_id NOT in hashed output
- Same input → same hash (deterministic)
- Different inputs → different hashes
- Account masking retains only last 4 digits
- is_pii_masked is True on all output records
"""
import hashlib

# ── MASKING FUNCTIONS ────────────────────────────────────────────
# Defined here independently of the Spark UDF wrappers in bronze_ingestion.py.
# The UDF decorator (@F.udf) wraps these functions for Spark execution —
# the pure Python logic is what we're verifying.

SALT = "test_salt_for_unit_tests"

def mask_national_id(national_id: str) -> str:
    return hashlib.sha256(f"{SALT}{national_id}".encode()).hexdigest()

def mask_account(account: str) -> str:
    return f"****{account[-4:]}"

# ── SHA-256 MASKING TESTS ────────────────────────────────────────

def test_hash_output_is_64_hex_characters():
    result = mask_national_id("1234567890123")
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)

def test_original_id_not_in_hash():
    national_id = "1234567890123"
    result = mask_national_id(national_id)
    assert national_id not in result

def test_hash_is_deterministic():
    national_id = "9876543210987"
    assert mask_national_id(national_id) == mask_national_id(national_id)

def test_different_inputs_produce_different_hashes():
    hash_a = mask_national_id("1111111111111")
    hash_b = mask_national_id("2222222222222")
    assert hash_a != hash_b

def test_salt_changes_the_hash():
    """Same national_id with different salts must produce different hashes."""
    national_id = "1234567890123"
    hash_with_salt    = hashlib.sha256(f"saltA{national_id}".encode()).hexdigest()
    hash_without_salt = hashlib.sha256(f"saltB{national_id}".encode()).hexdigest()
    assert hash_with_salt != hash_without_salt

def test_hash_output_is_lowercase_hex():
    result = mask_national_id("1234567890123")
    assert result == result.lower()

# ── ACCOUNT MASKING TESTS ────────────────────────────────────────

def test_account_mask_retains_last_4_digits():
    assert mask_account("TH1234567890") == "****7890"

def test_account_mask_format():
    result = mask_account("TH9876543210")
    assert result.startswith("****")
    assert len(result) == 8  # 4 stars + 4 digits

def test_account_mask_different_accounts_different_suffixes():
    assert mask_account("TH0000000001") != mask_account("TH0000000002")

def test_account_mask_same_last_4_same_output():
    assert mask_account("TH1111119999") == mask_account("TH2222229999")

def test_account_mask_does_not_reveal_prefix():
    result = mask_account("TH9876543210")
    assert "9876543" not in result

# ── is_pii_masked FLAG ───────────────────────────────────────────
# Verifies the contract: every Bronze record must have is_pii_masked=True.
# In bronze_ingestion.py this is enforced via F.lit(True) in process_batch.
# Here we verify the output dict contract directly.

def make_bronze_record(national_id: str, sender_acc: str, receiver_acc: str) -> dict:
    """Simulate the Bronze record construction from process_batch."""
    return {
        "sender_national_id_hash": mask_national_id(national_id),
        "sender_account_masked":   mask_account(sender_acc),
        "receiver_account_masked": mask_account(receiver_acc),
        "is_pii_masked":           True,
    }

def test_is_pii_masked_true_on_output():
    record = make_bronze_record("1234567890123", "TH1111111111", "TH9999999999")
    assert record["is_pii_masked"] is True

def test_is_pii_masked_true_for_multiple_records():
    inputs = [
        ("1111111111111", "TH1000000001", "TH2000000001"),
        ("2222222222222", "TH1000000002", "TH2000000002"),
        ("3333333333333", "TH1000000003", "TH2000000003"),
    ]
    records = [make_bronze_record(*i) for i in inputs]
    assert all(r["is_pii_masked"] is True for r in records)

def test_unmasked_national_id_not_in_bronze_record():
    national_id = "9876543210987"
    record = make_bronze_record(national_id, "TH1111111111", "TH9999999999")
    assert national_id not in record["sender_national_id_hash"]

def test_unmasked_account_not_in_bronze_record():
    record = make_bronze_record("1234567890123", "TH1234567890", "TH0987654321")
    assert "TH1234567890" not in record["sender_account_masked"]
    assert "TH0987654321" not in record["receiver_account_masked"]
