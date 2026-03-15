#!/usr/bin/env python3
"""
Debug script to check MOEX API responses
"""

import json
import urllib.request
import ssl

#!/usr/bin/env python3
"""
Debug script to check MOEX API responses
"""

import json
import urllib.request
import ssl

def test_api():
    """ Test the MOEX API directly """
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json"

    context = ssl.create_default_context()
    with urllib.request.urlopen(url, context=context) as response:
        data = json.loads(response.read().decode('utf-8'))

    print("Available columns:")
    if 'securities' in data:
        columns = data['securities']['columns']
        for i, col in enumerate(columns):
            print(f"{i}: {col}")

        print(f"\nTotal columns: {len(columns)}")

        # Look for SBER
        for row in data['securities']['data']:
            if row[columns.index('SECID')] == 'SBER':
                print(f"\nSBER data:")
                for i, val in enumerate(row):
                    print(f"  {columns[i]}: {val}")
                break

if __name__ == '__main__':
    test_api()

if __name__ == '__main__':
    test_api()