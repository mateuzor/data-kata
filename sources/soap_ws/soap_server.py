"""
SOAP WS-* Mock Server - Product Catalog Service
Uses spyne to expose a traditional SOAP/WSDL service.
"""

from spyne import Application, ServiceBase, Unicode, Integer, Float, rpc, Iterable, ComplexModel
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────

class Product(ComplexModel):
    product_id   = Integer
    name         = Unicode
    category     = Unicode
    unit_price   = Float
    is_active    = Unicode


class Salesman(ComplexModel):
    salesman_id = Integer
    name        = Unicode
    city        = Unicode
    state       = Unicode
    region      = Unicode


# ─────────────────────────────────────────────
# Static Data
# ─────────────────────────────────────────────

PRODUCTS = [
    (1,  "Notebook Dell Inspiron 15",      "Eletrônicos",      3499.90, "Y"),
    (2,  "Smartphone Samsung Galaxy A54",  "Eletrônicos",      1799.90, "Y"),
    (3,  "Monitor LG 27\" 4K",             "Eletrônicos",      2199.90, "Y"),
    (4,  "Teclado Mecânico Redragon",       "Periféricos",       399.90, "Y"),
    (5,  "Mouse Logitech MX Master 3",     "Periféricos",       599.90, "Y"),
    (6,  "Cadeira Gamer ThunderX3",        "Móveis",           1299.90, "Y"),
    (7,  "Headset JBL Quantum 400",        "Áudio",             449.90, "Y"),
    (8,  "Tablet iPad Air 5",              "Eletrônicos",      4999.90, "Y"),
    (9,  "Impressora HP LaserJet",         "Periféricos",       899.90, "Y"),
    (10, "SSD Kingston 1TB",               "Armazenamento",     399.90, "Y"),
    (11, "Webcam Logitech C920",           "Periféricos",       699.90, "Y"),
    (12, "Roteador TP-Link AX3000",        "Redes",             499.90, "Y"),
    (13, "Smart TV Samsung 55\" QLED",     "Eletrônicos",      4299.90, "Y"),
    (14, "Ar Condicionado Daikin 12k",     "Climatização",     3199.90, "Y"),
    (15, "Fritadeira Airfryer Philco",     "Eletrodomésticos",  399.90, "Y"),
]

SALESMEN = [
    (1,  "Carlos Andrade",    "São Paulo",      "SP", "Sudeste"),
    (2,  "Fernanda Lima",     "Rio de Janeiro", "RJ", "Sudeste"),
    (3,  "Roberto Souza",     "Belo Horizonte", "MG", "Sudeste"),
    (4,  "Patricia Mendes",   "Curitiba",       "PR", "Sul"),
    (5,  "Marcos Oliveira",   "Porto Alegre",   "RS", "Sul"),
    (6,  "Juliana Costa",     "Salvador",       "BA", "Nordeste"),
    (7,  "Anderson Ferreira", "Fortaleza",      "CE", "Nordeste"),
    (8,  "Camila Santos",     "Recife",         "PE", "Nordeste"),
    (9,  "Diego Alves",       "Manaus",         "AM", "Norte"),
    (10, "Thais Rodrigues",   "Brasília",       "DF", "Centro-Oeste"),
]


# ─────────────────────────────────────────────
# SOAP Service
# ─────────────────────────────────────────────

class ProductCatalogService(ServiceBase):
    """WS-* Product Catalog Service - exposes product and salesman data."""

    @rpc(_returns=Iterable(Product))
    def GetAllProducts(ctx):
        """Returns all products in the catalog."""
        for p in PRODUCTS:
            prod = Product()
            prod.product_id  = p[0]
            prod.name        = p[1]
            prod.category    = p[2]
            prod.unit_price  = p[3]
            prod.is_active   = p[4]
            yield prod

    @rpc(Integer, _returns=Product)
    def GetProductById(ctx, product_id):
        """Returns a single product by ID."""
        for p in PRODUCTS:
            if p[0] == product_id:
                prod = Product()
                prod.product_id  = p[0]
                prod.name        = p[1]
                prod.category    = p[2]
                prod.unit_price  = p[3]
                prod.is_active   = p[4]
                return prod
        return None

    @rpc(Unicode, _returns=Iterable(Product))
    def GetProductsByCategory(ctx, category):
        """Returns all products in a given category."""
        for p in PRODUCTS:
            if p[2].lower() == category.lower():
                prod = Product()
                prod.product_id  = p[0]
                prod.name        = p[1]
                prod.category    = p[2]
                prod.unit_price  = p[3]
                prod.is_active   = p[4]
                yield prod

    @rpc(_returns=Iterable(Salesman))
    def GetAllSalesmen(ctx):
        """Returns all salesmen registered."""
        for s in SALESMEN:
            sm = Salesman()
            sm.salesman_id = s[0]
            sm.name        = s[1]
            sm.city        = s[2]
            sm.state       = s[3]
            sm.region      = s[4]
            yield sm

    @rpc(Integer, _returns=Salesman)
    def GetSalesmanById(ctx, salesman_id):
        """Returns a single salesman by ID."""
        for s in SALESMEN:
            if s[0] == salesman_id:
                sm = Salesman()
                sm.salesman_id = s[0]
                sm.name        = s[1]
                sm.city        = s[2]
                sm.state       = s[3]
                sm.region      = s[4]
                return sm
        return None


# ─────────────────────────────────────────────
# App bootstrap
# ─────────────────────────────────────────────

application = Application(
    services=[ProductCatalogService],
    tns="http://data-kata.br/product-catalog",
    in_protocol=Soap11(validator="lxml"),
    out_protocol=Soap11(),
)

wsgi_app = WsgiApplication(application)

if __name__ == "__main__":
    port = int(__import__("os").environ.get("SOAP_WS_PORT", 8001))
    logger.info(f"Starting SOAP server on port {port}")
    logger.info(f"WSDL available at http://0.0.0.0:{port}/?wsdl")
    server = make_server("0.0.0.0", port, wsgi_app)
    server.serve_forever()
