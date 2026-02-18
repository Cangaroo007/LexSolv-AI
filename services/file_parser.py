"""
File parser for aged payables, balance sheets, bank statements, and P&L reports.

Supports CSV and Excel (.xlsx/.xls) formats with automatic column mapping
for Xero, MYOB, and generic accounting exports.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


class FileParser:
    """Parse common accounting file exports into normalised dicts."""

    # ------------------------------------------------------------------ #
    #  Aged Payables                                                      #
    # ------------------------------------------------------------------ #

    # Column mapping presets tried in order
    _PAYABLES_MAPPINGS: list[dict[str, str]] = [
        # Xero
        {"name": "Contact", "amount": "Total"},
        # MYOB
        {"name": "Co./Last Name", "amount": "Balance Due"},
        # Generic fallbacks — first match wins
        {"name": "Creditor", "amount": "Amount"},
        {"name": "Creditor", "amount": "Balance"},
        {"name": "Creditor", "amount": "Total"},
        {"name": "Supplier", "amount": "Amount"},
        {"name": "Supplier", "amount": "Balance"},
        {"name": "Supplier", "amount": "Total"},
        {"name": "Name", "amount": "Amount"},
        {"name": "Name", "amount": "Balance"},
        {"name": "Name", "amount": "Total"},
    ]

    def parse_aged_payables(self, file_path: str) -> list[dict]:
        """
        Parse aged payables CSV/Excel from Xero or MYOB.

        Try known column mappings in order:
        - Xero: 'Contact' -> name, 'Total' -> amount
        - MYOB: 'Co./Last Name' -> name, 'Balance Due' -> amount
        - Generic: 'Creditor'/'Supplier'/'Name' -> name,
                   'Amount'/'Balance'/'Total' -> amount

        If no mapping matches, raise ValueError with the column names found
        and a message suggesting the user check the file format.

        Returns: list of dicts with keys: creditor_name, amount_claimed,
                 category (if detectable)
        """
        df = self._read_file(file_path)

        # Try each mapping
        cols_lower = {c.lower(): c for c in df.columns}
        for mapping in self._PAYABLES_MAPPINGS:
            name_key = mapping["name"].lower()
            amount_key = mapping["amount"].lower()
            if name_key in cols_lower and amount_key in cols_lower:
                name_col = cols_lower[name_key]
                amount_col = cols_lower[amount_key]
                break
        else:
            raise ValueError(
                f"Could not identify creditor/amount columns. "
                f"Columns found: {list(df.columns)}. "
                f"Please check the file format — expected Xero, MYOB, "
                f"or a CSV with Creditor/Supplier/Name and Amount/Balance/Total columns."
            )

        results: list[dict] = []
        for _, row in df.iterrows():
            name = str(row[name_col]).strip()
            if not name or name.lower() == "nan":
                continue
            amount = self._parse_amount(row[amount_col])
            entry: dict = {
                "creditor_name": name,
                "amount_claimed": amount,
            }
            # Detect category from aging buckets if available
            category = self._detect_creditor_category(row, df.columns)
            if category:
                entry["category"] = category
            results.append(entry)

        return results

    # ------------------------------------------------------------------ #
    #  Balance Sheet                                                       #
    # ------------------------------------------------------------------ #

    _BALANCE_SHEET_KEYWORDS: dict[str, list[str]] = {
        "cash": ["cash", "bank", "cheque account", "savings"],
        "receivables": ["receivable", "trade debtor", "accounts receivable"],
        "inventory": ["inventory", "stock", "stock on hand"],
        "loans_shareholder": [
            "shareholder loan",
            "shareholder loans",
        ],
        "loans_to_related": [
            "loan to",
            "loans to",
            "loans - related",
            "related entities",
            "intercompany",
            "director loan",
        ],
        "equipment": ["plant", "equipment", "motor vehicle", "furniture"],
        "total_liabilities": ["total liabilities", "total current liabilities"],
    }

    def parse_balance_sheet(self, file_path: str) -> dict:
        """
        Parse balance sheet CSV/Excel.  Typically vertical layout:
        account name | amount.

        Extract by keyword matching into asset categories plus
        total_liabilities.

        Returns: dict with keys for each asset category + total_liabilities
        """
        df = self._read_file(file_path)

        # Identify the account-name and amount columns
        account_col, amount_col = self._identify_two_columns(df)

        result: dict = {
            "cash": 0.0,
            "receivables": 0.0,
            "inventory": 0.0,
            "loans_to_related": 0.0,
            "loans_shareholder": 0.0,
            "equipment": 0.0,
            "total_liabilities": 0.0,
        }

        for _, row in df.iterrows():
            account = str(row[account_col]).strip().lower()
            if not account or account == "nan":
                continue
            amount = self._parse_amount(row[amount_col])

            for category, keywords in self._BALANCE_SHEET_KEYWORDS.items():
                if any(kw in account for kw in keywords):
                    result[category] += amount
                    break

        return result

    # ------------------------------------------------------------------ #
    #  Bank Statement                                                      #
    # ------------------------------------------------------------------ #

    def parse_bank_statement(self, file_path: str) -> dict:
        """
        Parse bank statement CSV.
        Most bank CSVs have: Date, Description, Debit, Credit, Balance.

        Returns: dict with closing_balance, statement_period
                 (start_date, end_date)
        """
        df = self._read_file(file_path)
        cols_lower = {c.lower(): c for c in df.columns}

        # Find balance column
        balance_col = None
        for candidate in ["balance", "closing balance", "running balance"]:
            if candidate in cols_lower:
                balance_col = cols_lower[candidate]
                break

        # Find date column
        date_col = None
        for candidate in ["date", "transaction date", "value date", "posting date"]:
            if candidate in cols_lower:
                date_col = cols_lower[candidate]
                break

        if balance_col is None or date_col is None:
            raise ValueError(
                f"Could not identify date/balance columns. "
                f"Columns found: {list(df.columns)}. "
                f"Expected a bank statement with Date and Balance columns."
            )

        # Parse dates
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
        df = df.dropna(subset=[date_col])

        if df.empty:
            raise ValueError("No valid date entries found in the bank statement.")

        df = df.sort_values(date_col)

        closing_balance = self._parse_amount(df[balance_col].iloc[-1])
        start_date = df[date_col].iloc[0].strftime("%Y-%m-%d")
        end_date = df[date_col].iloc[-1].strftime("%Y-%m-%d")

        return {
            "closing_balance": closing_balance,
            "statement_period": {
                "start_date": start_date,
                "end_date": end_date,
            },
        }

    # ------------------------------------------------------------------ #
    #  Profit & Loss                                                       #
    # ------------------------------------------------------------------ #

    def parse_pnl(self, file_path: str) -> dict:
        """
        Parse P&L / income statement CSV/Excel.
        Look for revenue/income rows and net profit rows.

        Returns: dict with revenue_by_year, profit_by_year
        """
        df = self._read_file(file_path)
        account_col, _ = self._identify_two_columns(df)

        # Identify year columns (numeric column headers or columns after the account col)
        year_cols: list[str] = []
        for col in df.columns:
            if col == account_col:
                continue
            # Check if column name looks like a year
            col_stripped = str(col).strip()
            if re.match(r"^\d{4}$", col_stripped):
                year_cols.append(col)
            elif col_stripped.replace(".", "").replace(",", "").replace("$", "").strip():
                year_cols.append(col)

        revenue_by_year: dict[str, float] = {}
        profit_by_year: dict[str, float] = {}

        revenue_keywords = ["revenue", "income", "sales", "turnover", "total revenue", "total income"]
        profit_keywords = ["net profit", "net income", "net loss", "profit after tax", "profit before tax"]

        for _, row in df.iterrows():
            account = str(row[account_col]).strip().lower()
            if not account or account == "nan":
                continue

            for year_col in year_cols:
                year_label = str(year_col).strip()
                amount = self._parse_amount(row[year_col])

                if any(kw in account for kw in revenue_keywords):
                    revenue_by_year[year_label] = revenue_by_year.get(year_label, 0.0) + amount
                elif any(kw in account for kw in profit_keywords):
                    profit_by_year[year_label] = profit_by_year.get(year_label, 0.0) + amount

        return {
            "revenue_by_year": revenue_by_year,
            "profit_by_year": profit_by_year,
        }

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _read_file(self, file_path: str) -> pd.DataFrame:
        """Read CSV or Excel into DataFrame based on file extension."""
        path = Path(file_path)
        ext = path.suffix.lower()

        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(file_path, engine="openpyxl")
        else:
            raise ValueError(
                f"Unsupported file format: '{ext}'. "
                f"Please provide a .csv, .xlsx, or .xls file."
            )

        # Strip whitespace from column headers
        df.columns = [str(c).strip() for c in df.columns]
        return df

    @staticmethod
    def _parse_amount(value) -> float:
        """
        Parse a monetary value, handling currency symbols and commas.
        "$1,234.56" -> 1234.56
        """
        if pd.isna(value):
            return 0.0
        s = str(value).strip()
        if not s:
            return 0.0
        # Detect negative: parentheses or leading minus
        negative = False
        if s.startswith("(") and s.endswith(")"):
            negative = True
            s = s[1:-1]
        # Remove currency symbols and commas
        s = re.sub(r"[£€¥$,]", "", s).strip()
        if not s or s == "-":
            return 0.0
        try:
            result = float(s)
        except ValueError:
            return 0.0
        return -result if negative else result

    @staticmethod
    def _detect_creditor_category(row: pd.Series, columns: pd.Index) -> str | None:
        """Infer creditor category from row data if possible (e.g. aging buckets)."""
        # If we have aging columns and can detect the bucket
        cols_lower = {str(c).lower(): c for c in columns}
        aging_90 = None
        for candidate in ["90+ days", "90 days", "91+ days"]:
            if candidate in cols_lower:
                aging_90 = cols_lower[candidate]
                break

        if aging_90 is not None:
            try:
                val = float(str(row[aging_90]).replace("$", "").replace(",", ""))
                if val > 0:
                    return "aged_90_plus"
            except (ValueError, TypeError):
                pass
        return None

    @staticmethod
    def _identify_two_columns(df: pd.DataFrame) -> tuple[str, str]:
        """
        For vertical-format reports (balance sheet, P&L), identify the
        account-name column and the first numeric-value column.
        """
        account_col = df.columns[0]
        amount_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        return account_col, amount_col

    def _ai_identify_columns(self, df: pd.DataFrame, data_type: str) -> list[dict]:
        """
        STUB — will use Claude API when built in Prompt 2.2.
        For now: raise ValueError with helpful message about unrecognised format.

        # REQUIRES: Claude API integration (Prompt 2.2) — stub until then
        """
        raise ValueError(
            f"Could not automatically identify columns for {data_type}. "
            f"Columns found: {list(df.columns)}. "
            f"AI-assisted column identification will be available in a future update. "
            f"Please ensure your file uses standard column names."
        )
