"""
Example SOAP client using zeep - shows how to consume the WS-* service.
Used by the Kafka producer to pull product/salesman data.
"""

import zeep
import logging

logging.basicConfig(level=logging.INFO)

WSDL_URL = "http://soap-server:8001/?wsdl"


def get_client(wsdl_url: str = WSDL_URL) -> zeep.Client:
    return zeep.Client(wsdl=wsdl_url)


def fetch_all_products(client: zeep.Client) -> list[dict]:
    result = client.service.GetAllProducts()
    return [
        {
            "product_id":  p.product_id,
            "name":        p.name,
            "category":    p.category,
            "unit_price":  float(p.unit_price),
            "is_active":   p.is_active,
        }
        for p in (result or [])
    ]


def fetch_all_salesmen(client: zeep.Client) -> list[dict]:
    result = client.service.GetAllSalesmen()
    return [
        {
            "salesman_id": s.salesman_id,
            "name":        s.name,
            "city":        s.city,
            "state":       s.state,
            "region":      s.region,
        }
        for s in (result or [])
    ]


if __name__ == "__main__":
    import os
    wsdl = f"http://{os.getenv('SOAP_WS_HOST', 'localhost')}:{os.getenv('SOAP_WS_PORT', '8001')}/?wsdl"
    client = get_client(wsdl)

    print("=== Products ===")
    for p in fetch_all_products(client):
        print(p)

    print("\n=== Salesmen ===")
    for s in fetch_all_salesmen(client):
        print(s)
