import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from src.extractors.fundamental_extractor import FundamentalExtractor


class TestFundamentalExtractor(unittest.TestCase):
    """Test cases cho FundamentalExtractor (mock vnstock, không gọi mạng)."""

    def setUp(self) -> None:
        self.extractor = FundamentalExtractor()

    def test_income_statement_vnm_has_data(self) -> None:
        """Lấy Income Statement cho VNM (có dữ liệu)."""

        mocked_income_statement = Mock(
            return_value=pd.DataFrame(
            [
                {'time': '2024Q4', 'revenue': 1_000, 'profit': 100, 'eps': 2.5},
                {'time': '2024Q3', 'revenue': 900, 'profit': 90, 'eps': 2.2},
            ]
        )
        )
        fake_vnstock = SimpleNamespace(financial=SimpleNamespace(income_statement=mocked_income_statement))

        with patch('src.extractors.fundamental_extractor.vnstock', fake_vnstock):
            df = self.extractor.get_income_statement(symbol='VNM', period='quarterly', get_all=True)
        self.assertFalse(df.empty)
        self.assertGreaterEqual(len(df), 2)

    def test_income_statement_new_listing_no_data(self) -> None:
        """Lấy Income Statement cho mã mới niêm yết (không có dữ liệu)."""

        mocked_income_statement = Mock(return_value=pd.DataFrame())
        fake_vnstock = SimpleNamespace(financial=SimpleNamespace(income_statement=mocked_income_statement))

        with patch('src.extractors.fundamental_extractor.vnstock', fake_vnstock):
            df = self.extractor.get_income_statement(symbol='ABC', period='quarterly', get_all=True)
        self.assertTrue(df.empty)

    def test_income_statement_required_columns_present(self) -> None:
        """Kiểm tra các cột bắt buộc trong kết quả trả về."""

        mocked_income_statement = Mock(
            return_value=pd.DataFrame(
                [{'time': '2024Q4', 'revenue': 1_000, 'profit': 100, 'eps': 2.5}]
            )
        )
        fake_vnstock = SimpleNamespace(financial=SimpleNamespace(income_statement=mocked_income_statement))

        with patch('src.extractors.fundamental_extractor.vnstock', fake_vnstock):
            df = self.extractor.get_income_statement(symbol='VNM', period='quarterly', get_all=True)

        required_columns = {'time', 'revenue', 'profit', 'eps'}
        self.assertTrue(required_columns.issubset(df.columns))


if __name__ == '__main__':
    unittest.main()
