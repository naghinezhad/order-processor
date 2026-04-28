CREATE TABLE IF NOT EXISTS order_requests (
  request_id UUID PRIMARY KEY,
  user_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  quantity INT NOT NULL,
  status TEXT NOT NULL,
  order_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
  id BIGSERIAL PRIMARY KEY,
  request_id UUID NOT NULL UNIQUE,
  user_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  quantity INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id UUID PRIMARY KEY,
  request_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_requests_status ON order_requests (status);
CREATE INDEX IF NOT EXISTS idx_orders_request_id ON orders (request_id);
