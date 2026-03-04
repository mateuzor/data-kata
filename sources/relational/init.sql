-- ============================================================
-- SOURCE DATABASE - Schema + Seed Data (Mar 3-9, 2026)
-- ============================================================

-- Enable logical replication for Debezium CDC
ALTER SYSTEM SET wal_level = logical;

-- ─────────────────────────────────────────────
-- TABLES
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS salesmen (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    city        VARCHAR(100) NOT NULL,
    state       CHAR(2)      NOT NULL,
    region      VARCHAR(50)  NOT NULL,
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    category    VARCHAR(80)  NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL,
    created_at  TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sales (
    id           SERIAL PRIMARY KEY,
    salesman_id  INT           NOT NULL REFERENCES salesmen(id),
    product_id   INT           NOT NULL REFERENCES products(id),
    city         VARCHAR(100)  NOT NULL,
    state        CHAR(2)       NOT NULL,
    quantity     INT           NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12,2) NOT NULL,
    total_amount NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    sale_date    DATE          NOT NULL,
    created_at   TIMESTAMP     DEFAULT NOW()
);

CREATE INDEX idx_sales_date     ON sales(sale_date);
CREATE INDEX idx_sales_city     ON sales(city);
CREATE INDEX idx_sales_salesman ON sales(salesman_id);

-- ─────────────────────────────────────────────
-- SALESMEN (20 vendedores em todo o Brasil)
-- ─────────────────────────────────────────────

INSERT INTO salesmen (name, city, state, region) VALUES
  ('Carlos Andrade',      'São Paulo',        'SP', 'Sudeste'),
  ('Fernanda Lima',       'Rio de Janeiro',   'RJ', 'Sudeste'),
  ('Roberto Souza',       'Belo Horizonte',   'MG', 'Sudeste'),
  ('Patricia Mendes',     'Curitiba',         'PR', 'Sul'),
  ('Marcos Oliveira',     'Porto Alegre',     'RS', 'Sul'),
  ('Juliana Costa',       'Salvador',         'BA', 'Nordeste'),
  ('Anderson Ferreira',   'Fortaleza',        'CE', 'Nordeste'),
  ('Camila Santos',       'Recife',           'PE', 'Nordeste'),
  ('Diego Alves',         'Manaus',           'AM', 'Norte'),
  ('Thais Rodrigues',     'Brasília',         'DF', 'Centro-Oeste'),
  ('Rafael Nascimento',   'Goiânia',          'GO', 'Centro-Oeste'),
  ('Beatriz Carvalho',    'Campo Grande',     'MS', 'Centro-Oeste'),
  ('Gustavo Pereira',     'São Paulo',        'SP', 'Sudeste'),
  ('Aline Martins',       'Campinas',         'SP', 'Sudeste'),
  ('Leonardo Gomes',      'Santos',           'SP', 'Sudeste'),
  ('Vanessa Rocha',       'Florianópolis',    'SC', 'Sul'),
  ('Felipe Araújo',       'Belém',            'PA', 'Norte'),
  ('Natalia Vieira',      'São Luís',         'MA', 'Nordeste'),
  ('Bruno Teixeira',      'Maceió',           'AL', 'Nordeste'),
  ('Mariana Barbosa',     'Vitória',          'ES', 'Sudeste');

-- ─────────────────────────────────────────────
-- PRODUCTS
-- ─────────────────────────────────────────────

INSERT INTO products (name, category, unit_price) VALUES
  ('Notebook Dell Inspiron 15',     'Eletrônicos',   3499.90),
  ('Smartphone Samsung Galaxy A54', 'Eletrônicos',   1799.90),
  ('Monitor LG 27" 4K',             'Eletrônicos',   2199.90),
  ('Teclado Mecânico Redragon',      'Periféricos',    399.90),
  ('Mouse Logitech MX Master 3',    'Periféricos',    599.90),
  ('Cadeira Gamer ThunderX3',       'Móveis',        1299.90),
  ('Headset JBL Quantum 400',       'Áudio',          449.90),
  ('Tablet iPad Air 5',             'Eletrônicos',   4999.90),
  ('Impressora HP LaserJet',        'Periféricos',    899.90),
  ('SSD Kingston 1TB',              'Armazenamento',  399.90),
  ('Webcam Logitech C920',          'Periféricos',    699.90),
  ('Roteador TP-Link AX3000',       'Redes',          499.90),
  ('Smart TV Samsung 55" QLED',     'Eletrônicos',   4299.90),
  ('Ar Condicionado Daikin 12k',    'Climatização',  3199.90),
  ('Fritadeira Airfryer Philco',    'Eletrodomésticos', 399.90);

-- ─────────────────────────────────────────────
-- SALES - March 3, 2026
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  1,  'São Paulo',       'SP', 3, 3499.90, '2026-03-03'),
  (1,  3,  'São Paulo',       'SP', 2, 2199.90, '2026-03-03'),
  (2,  2,  'Rio de Janeiro',  'RJ', 5, 1799.90, '2026-03-03'),
  (2,  8,  'Rio de Janeiro',  'RJ', 1, 4999.90, '2026-03-03'),
  (3,  13, 'Belo Horizonte',  'MG', 2, 4299.90, '2026-03-03'),
  (4,  6,  'Curitiba',        'PR', 4, 1299.90, '2026-03-03'),
  (5,  5,  'Porto Alegre',    'RS', 6, 599.90,  '2026-03-03'),
  (6,  2,  'Salvador',        'BA', 3, 1799.90, '2026-03-03'),
  (7,  1,  'Fortaleza',       'CE', 2, 3499.90, '2026-03-03'),
  (8,  4,  'Recife',          'PE', 8, 399.90,  '2026-03-03'),
  (9,  11, 'Manaus',          'AM', 2, 699.90,  '2026-03-03'),
  (10, 14, 'Brasília',        'DF', 1, 3199.90, '2026-03-03'),
  (13, 1,  'São Paulo',       'SP', 4, 3499.90, '2026-03-03'),
  (14, 2,  'Campinas',        'SP', 7, 1799.90, '2026-03-03'),
  (15, 3,  'Santos',          'SP', 3, 2199.90, '2026-03-03');

-- ─────────────────────────────────────────────
-- SALES - March 4, 2026
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  8,  'São Paulo',       'SP', 2, 4999.90, '2026-03-04'),
  (2,  13, 'Rio de Janeiro',  'RJ', 3, 4299.90, '2026-03-04'),
  (3,  1,  'Belo Horizonte',  'MG', 5, 3499.90, '2026-03-04'),
  (4,  2,  'Curitiba',        'PR', 4, 1799.90, '2026-03-04'),
  (5,  14, 'Porto Alegre',    'RS', 2, 3199.90, '2026-03-04'),
  (6,  6,  'Salvador',        'BA', 3, 1299.90, '2026-03-04'),
  (7,  3,  'Fortaleza',       'CE', 4, 2199.90, '2026-03-04'),
  (8,  9,  'Recife',          'PE', 2, 899.90,  '2026-03-04'),
  (9,  12, 'Manaus',          'AM', 3, 499.90,  '2026-03-04'),
  (10, 1,  'Brasília',        'DF', 5, 3499.90, '2026-03-04'),
  (11, 2,  'Goiânia',         'GO', 6, 1799.90, '2026-03-04'),
  (12, 5,  'Campo Grande',    'MS', 4, 599.90,  '2026-03-04'),
  (16, 13, 'Florianópolis',   'SC', 2, 4299.90, '2026-03-04'),
  (17, 1,  'Belém',           'PA', 3, 3499.90, '2026-03-04'),
  (18, 4,  'São Luís',        'MA', 5, 399.90,  '2026-03-04');

-- ─────────────────────────────────────────────
-- SALES - March 5, 2026
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  13, 'São Paulo',       'SP', 3, 4299.90, '2026-03-05'),
  (2,  1,  'Rio de Janeiro',  'RJ', 4, 3499.90, '2026-03-05'),
  (3,  8,  'Belo Horizonte',  'MG', 2, 4999.90, '2026-03-05'),
  (4,  13, 'Curitiba',        'PR', 1, 4299.90, '2026-03-05'),
  (5,  1,  'Porto Alegre',    'RS', 6, 3499.90, '2026-03-05'),
  (6,  3,  'Salvador',        'BA', 3, 2199.90, '2026-03-05'),
  (7,  2,  'Fortaleza',       'CE', 5, 1799.90, '2026-03-05'),
  (8,  6,  'Recife',          'PE', 4, 1299.90, '2026-03-05'),
  (9,  1,  'Manaus',          'AM', 2, 3499.90, '2026-03-05'),
  (10, 8,  'Brasília',        'DF', 3, 4999.90, '2026-03-05'),
  (13, 3,  'São Paulo',       'SP', 4, 2199.90, '2026-03-05'),
  (14, 8,  'Campinas',        'SP', 2, 4999.90, '2026-03-05'),
  (19, 2,  'Maceió',          'AL', 3, 1799.90, '2026-03-05'),
  (20, 14, 'Vitória',         'ES', 2, 3199.90, '2026-03-05'),
  (15, 13, 'Santos',          'SP', 3, 4299.90, '2026-03-05');

-- ─────────────────────────────────────────────
-- SALES - March 6, 2026
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  2,  'São Paulo',       'SP', 8, 1799.90, '2026-03-06'),
  (2,  6,  'Rio de Janeiro',  'RJ', 3, 1299.90, '2026-03-06'),
  (3,  2,  'Belo Horizonte',  'MG', 4, 1799.90, '2026-03-06'),
  (4,  1,  'Curitiba',        'PR', 3, 3499.90, '2026-03-06'),
  (5,  8,  'Porto Alegre',    'RS', 2, 4999.90, '2026-03-06'),
  (6,  1,  'Salvador',        'BA', 4, 3499.90, '2026-03-06'),
  (7,  13, 'Fortaleza',       'CE', 2, 4299.90, '2026-03-06'),
  (8,  2,  'Recife',          'PE', 5, 1799.90, '2026-03-06'),
  (9,  3,  'Manaus',          'AM', 3, 2199.90, '2026-03-06'),
  (10, 6,  'Brasília',        'DF', 4, 1299.90, '2026-03-06'),
  (11, 8,  'Goiânia',         'GO', 2, 4999.90, '2026-03-06'),
  (12, 13, 'Campo Grande',    'MS', 2, 4299.90, '2026-03-06'),
  (16, 1,  'Florianópolis',   'SC', 5, 3499.90, '2026-03-06'),
  (17, 2,  'Belém',           'PA', 4, 1799.90, '2026-03-06'),
  (18, 3,  'São Luís',        'MA', 3, 2199.90, '2026-03-06');

-- ─────────────────────────────────────────────
-- SALES - March 7, 2026 (weekend - lower volume)
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  15, 'São Paulo',       'SP', 5, 399.90,  '2026-03-07'),
  (2,  4,  'Rio de Janeiro',  'RJ', 6, 399.90,  '2026-03-07'),
  (3,  5,  'Belo Horizonte',  'MG', 4, 599.90,  '2026-03-07'),
  (5,  7,  'Porto Alegre',    'RS', 3, 449.90,  '2026-03-07'),
  (6,  10, 'Salvador',        'BA', 5, 399.90,  '2026-03-07'),
  (10, 5,  'Brasília',        'DF', 2, 599.90,  '2026-03-07'),
  (13, 4,  'São Paulo',       'SP', 7, 399.90,  '2026-03-07'),
  (14, 7,  'Campinas',        'SP', 4, 449.90,  '2026-03-07');

-- ─────────────────────────────────────────────
-- SALES - March 8, 2026 (weekend)
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  1,  'São Paulo',       'SP', 5, 3499.90, '2026-03-08'),
  (2,  8,  'Rio de Janeiro',  'RJ', 2, 4999.90, '2026-03-08'),
  (3,  13, 'Belo Horizonte',  'MG', 3, 4299.90, '2026-03-08'),
  (4,  1,  'Curitiba',        'PR', 2, 3499.90, '2026-03-08'),
  (7,  8,  'Fortaleza',       'CE', 3, 4999.90, '2026-03-08'),
  (8,  3,  'Recife',          'PE', 4, 2199.90, '2026-03-08'),
  (11, 1,  'Goiânia',         'GO', 3, 3499.90, '2026-03-08'),
  (20, 13, 'Vitória',         'ES', 2, 4299.90, '2026-03-08');

-- ─────────────────────────────────────────────
-- SALES - March 9, 2026 (Monday)
-- ─────────────────────────────────────────────

INSERT INTO sales (salesman_id, product_id, city, state, quantity, unit_price, sale_date) VALUES
  (1,  1,  'São Paulo',       'SP', 6, 3499.90, '2026-03-09'),
  (1,  8,  'São Paulo',       'SP', 3, 4999.90, '2026-03-09'),
  (2,  13, 'Rio de Janeiro',  'RJ', 4, 4299.90, '2026-03-09'),
  (2,  1,  'Rio de Janeiro',  'RJ', 5, 3499.90, '2026-03-09'),
  (3,  2,  'Belo Horizonte',  'MG', 6, 1799.90, '2026-03-09'),
  (4,  8,  'Curitiba',        'PR', 3, 4999.90, '2026-03-09'),
  (5,  13, 'Porto Alegre',    'RS', 4, 4299.90, '2026-03-09'),
  (6,  1,  'Salvador',        'BA', 5, 3499.90, '2026-03-09'),
  (7,  2,  'Fortaleza',       'CE', 7, 1799.90, '2026-03-09'),
  (8,  13, 'Recife',          'PE', 3, 4299.90, '2026-03-09'),
  (9,  8,  'Manaus',          'AM', 2, 4999.90, '2026-03-09'),
  (10, 1,  'Brasília',        'DF', 6, 3499.90, '2026-03-09'),
  (11, 2,  'Goiânia',         'GO', 5, 1799.90, '2026-03-09'),
  (13, 8,  'São Paulo',       'SP', 4, 4999.90, '2026-03-09'),
  (14, 1,  'Campinas',        'SP', 6, 3499.90, '2026-03-09'),
  (15, 13, 'Santos',          'SP', 3, 4299.90, '2026-03-09'),
  (16, 2,  'Florianópolis',   'SC', 4, 1799.90, '2026-03-09'),
  (17, 8,  'Belém',           'PA', 2, 4999.90, '2026-03-09'),
  (19, 1,  'Maceió',          'AL', 3, 3499.90, '2026-03-09'),
  (20, 3,  'Vitória',         'ES', 4, 2199.90, '2026-03-09');
