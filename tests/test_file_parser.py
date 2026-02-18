"""Tests for services.file_parser — FileParser."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from services.file_parser import FileParser

FIXTURES = Path(__file__).parent / "fixtures"

parser = FileParser()


# ------------------------------------------------------------------ #
#  Aged Payables                                                      #
# ------------------------------------------------------------------ #


class TestParseXeroAgedPayables:
    """test_parse_xero_aged_payables — 6 creditors with correct amounts."""

    def test_extracts_six_creditors(self):
        result = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))
        assert len(result) == 6

    def test_creditor_names(self):
        result = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))
        names = [r["creditor_name"] for r in result]
        assert "Australian Taxation Office - ITA" in names
        assert "Australian Taxation Office - ICA" in names
        assert "iCare NSW" in names
        assert "Prospa Advance" in names
        assert "BlueShak" in names
        assert "BTC Health Australia" in names

    def test_amounts_correct(self):
        result = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))
        by_name = {r["creditor_name"]: r for r in result}
        assert by_name["Australian Taxation Office - ITA"]["amount_claimed"] == 573230.31
        assert by_name["Australian Taxation Office - ICA"]["amount_claimed"] == 268294.01
        assert by_name["iCare NSW"]["amount_claimed"] == 825.23
        assert by_name["Prospa Advance"]["amount_claimed"] == 143874.02
        assert by_name["BlueShak"]["amount_claimed"] == 142105.81
        assert by_name["BTC Health Australia"]["amount_claimed"] == 67447.99

    def test_returns_dicts(self):
        result = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))
        for entry in result:
            assert isinstance(entry, dict)
            assert "creditor_name" in entry
            assert "amount_claimed" in entry

    def test_amounts_are_float(self):
        result = parser.parse_aged_payables(str(FIXTURES / "pbm_aged_payables.csv"))
        for entry in result:
            assert isinstance(entry["amount_claimed"], float)


# ------------------------------------------------------------------ #
#  Balance Sheet                                                       #
# ------------------------------------------------------------------ #


class TestParseBalanceSheet:
    """test_parse_balance_sheet_extracts_categories — cash, receivables, inventory."""

    def test_cash(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assert result["cash"] == pytest.approx(59689.27)

    def test_receivables(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assert result["receivables"] == pytest.approx(69553.24)

    def test_inventory(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assert result["inventory"] == pytest.approx(51826.62)

    def test_loans_to_related(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        # "Loans to Related Entities" + "Shareholder Loans"
        assert result["loans_to_related"] == pytest.approx(34964.83 + 2010000.00)

    def test_equipment(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assert result["equipment"] == pytest.approx(15000.00)

    def test_total_liabilities(self):
        result = parser.parse_balance_sheet(str(FIXTURES / "pbm_balance_sheet.csv"))
        assert result["total_liabilities"] == pytest.approx(985777.37)


# ------------------------------------------------------------------ #
#  Unknown Format → ValueError                                        #
# ------------------------------------------------------------------ #


class TestUnknownFormatRaisesError:
    """test_unknown_format_raises_error — unrecognised columns → ValueError."""

    def test_raises_with_helpful_message(self):
        # Create a CSV with columns that don't match any known mapping
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("Foo,Bar,Baz\n")
            f.write("a,1,2\n")
            f.write("b,3,4\n")
            tmp_path = f.name

        try:
            with pytest.raises(ValueError, match="Could not identify"):
                parser.parse_aged_payables(tmp_path)
        finally:
            os.unlink(tmp_path)

    def test_error_includes_column_names(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("Alpha,Beta,Gamma\n")
            f.write("x,1,2\n")
            tmp_path = f.name

        try:
            with pytest.raises(ValueError, match="Alpha"):
                parser.parse_aged_payables(tmp_path)
        finally:
            os.unlink(tmp_path)


# ------------------------------------------------------------------ #
#  Currency Formatting                                                 #
# ------------------------------------------------------------------ #


class TestHandlesCurrencyFormatting:
    """test_handles_currency_formatting — '$1,234.56' parses as 1234.56."""

    def test_dollar_sign_and_commas(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("Contact,Total\n")
            f.write('Acme Corp,"$1,234.56"\n')
            f.write('Big Co,"$99,999.00"\n')
            f.write("Small Pty Ltd,$50.00\n")
            tmp_path = f.name

        try:
            result = parser.parse_aged_payables(tmp_path)
            by_name = {r["creditor_name"]: r for r in result}
            assert by_name["Acme Corp"]["amount_claimed"] == 1234.56
            assert by_name["Big Co"]["amount_claimed"] == 99999.00
            assert by_name["Small Pty Ltd"]["amount_claimed"] == 50.00
        finally:
            os.unlink(tmp_path)

    def test_parentheses_negative(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("Contact,Total\n")
            f.write('Refund Co,"($500.00)"\n')
            tmp_path = f.name

        try:
            result = parser.parse_aged_payables(tmp_path)
            assert result[0]["amount_claimed"] == -500.00
        finally:
            os.unlink(tmp_path)


# ------------------------------------------------------------------ #
#  Excel (.xlsx) Support                                               #
# ------------------------------------------------------------------ #


class TestHandlesXlsx:
    """test_handles_xlsx — create .xlsx programmatically, verify it reads."""

    def test_reads_xlsx_aged_payables(self):
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Contact", "Current", "Total"])
        ws.append(["Alpha Ltd", 1000, 1000])
        ws.append(["Beta Pty", 2500.50, 2500.50])
        ws.append(["Gamma Inc", 300, 300])

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            tmp_path = f.name
            wb.save(tmp_path)

        try:
            result = parser.parse_aged_payables(tmp_path)
            assert len(result) == 3
            by_name = {r["creditor_name"]: r for r in result}
            assert by_name["Alpha Ltd"]["amount_claimed"] == 1000.0
            assert by_name["Beta Pty"]["amount_claimed"] == 2500.50
            assert by_name["Gamma Inc"]["amount_claimed"] == 300.0
        finally:
            os.unlink(tmp_path)

    def test_reads_xlsx_balance_sheet(self):
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Account", "Amount"])
        ws.append(["Cash at Bank", 10000])
        ws.append(["Accounts Receivable", 5000])
        ws.append(["Total Liabilities", 8000])

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            tmp_path = f.name
            wb.save(tmp_path)

        try:
            result = parser.parse_balance_sheet(tmp_path)
            assert result["cash"] == pytest.approx(10000.0)
            assert result["receivables"] == pytest.approx(5000.0)
            assert result["total_liabilities"] == pytest.approx(8000.0)
        finally:
            os.unlink(tmp_path)

    def test_unsupported_extension_raises(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            f.write(b"{}")
            tmp_path = f.name

        try:
            with pytest.raises(ValueError, match="Unsupported file format"):
                parser.parse_aged_payables(tmp_path)
        finally:
            os.unlink(tmp_path)
