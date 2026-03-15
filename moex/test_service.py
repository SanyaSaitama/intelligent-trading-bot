#!/usr/bin/env python3
"""
Test script for MOEX service
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from moex_service import MOEXQuotesService

def test_service():
    """ Test the service with a single fetch """
    service = MOEXQuotesService('test_moex_quotes.db')

    # Test fetching data for one security
    print("Testing data fetch for SBER...")
    service.fetch_security_data('SBER')

    print("Test completed. Check test_moex_quotes.db for data.")

if __name__ == '__main__':
    test_service()