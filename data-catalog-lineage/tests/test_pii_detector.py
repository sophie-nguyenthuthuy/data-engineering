import pytest
from catalog.pii_detector import detect_pii, detect_pii_from_name, detect_pii_from_values


class TestDetectFromName:
    def test_email_column(self):
        assert "EMAIL" in detect_pii_from_name("email_address")

    def test_phone_column(self):
        assert "PHONE" in detect_pii_from_name("phone_number")

    def test_ssn_column(self):
        assert "SSN" in detect_pii_from_name("ssn")

    def test_first_name(self):
        assert "NAME" in detect_pii_from_name("first_name")

    def test_last_name(self):
        assert "NAME" in detect_pii_from_name("last_name")

    def test_dob_column(self):
        assert "DATE_OF_BIRTH" in detect_pii_from_name("date_of_birth")

    def test_address_column(self):
        assert "ADDRESS" in detect_pii_from_name("billing_address")

    def test_zip_code(self):
        assert "ZIP_CODE" in detect_pii_from_name("zip_code")

    def test_credit_card(self):
        assert "CREDIT_CARD" in detect_pii_from_name("credit_card_number")

    def test_non_pii(self):
        assert detect_pii_from_name("product_id") == []
        assert detect_pii_from_name("created_at") == []
        assert detect_pii_from_name("order_total") == []

    def test_password(self):
        assert "CREDENTIAL" in detect_pii_from_name("password_hash")

    def test_ip_address(self):
        assert "IP_ADDRESS" in detect_pii_from_name("ip_address")


class TestDetectFromValues:
    def test_email_values(self):
        assert "EMAIL" in detect_pii_from_values(["alice@example.com", "bob@test.org"])

    def test_ssn_values(self):
        assert "SSN" in detect_pii_from_values(["123-45-6789"])

    def test_phone_values(self):
        assert "PHONE" in detect_pii_from_values(["555-867-5309"])

    def test_ip_values(self):
        assert "IP_ADDRESS" in detect_pii_from_values(["192.168.1.1"])

    def test_non_pii_values(self):
        tags = detect_pii_from_values(["electronics", "furniture", "clothing"])
        assert tags == []

    def test_none_values(self):
        tags = detect_pii_from_values([None, None])
        assert tags == []


class TestDetectCombined:
    def test_pii_prefix_added(self):
        tags = detect_pii("email")
        assert tags[0] == "PII"
        assert "EMAIL" in tags

    def test_no_duplicates(self):
        tags = detect_pii("email", ["alice@example.com"])
        assert tags.count("EMAIL") == 1
        assert tags.count("PII") == 1

    def test_non_pii_returns_empty(self):
        tags = detect_pii("product_id", ["1", "2", "3"])
        assert tags == []
