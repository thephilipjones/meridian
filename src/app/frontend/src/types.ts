export interface Profile {
  id: string;
  name: string;
  role: string;
  persona: string;
  avatar_initials: string;
  nav_tabs: string[];
  business_unit: string;
  genie_space_id: string | null;
}

export interface Article {
  article_id: string;
  doi: string | null;
  title: string;
  abstract?: string;
  journal: string | null;
  publication_date: string | null;
  publication_year: number | null;
  source: string;
  is_preprint: string;
  publication_type: string | null;
  citation_count: number;
}

export interface Author {
  author_id: string;
  full_name: string;
  last_name: string | null;
  first_name: string | null;
  article_count: number;
  h_index: number;
  first_pub_year: number | null;
  last_pub_year: number | null;
}

export interface SalesPipeline {
  stage: string;
  deal_count: number;
  total_amount: number;
  total_arr: number;
  avg_deal_size: number;
  conversion_rate: number;
  product_line: string;
  region: string;
  fiscal_quarter: string;
}

export interface ProductUsage {
  account_name: string;
  product: string;
  period: string;
  api_calls: number;
  unique_users: number;
  avg_response_ms: number;
  error_rate: number;
  bytes_served: number;
}

export interface RevenueSummary {
  fiscal_quarter: string;
  fiscal_year: number;
  product_line: string;
  revenue: number;
  cost_of_data: number;
  gross_margin: number;
  gross_margin_pct: number;
  customer_count: number;
  revenue_per_customer: number;
  yoy_revenue_growth: number | null;
}

export interface CustomerHealth {
  account_name: string;
  account_id: string;
  arr: number;
  products_subscribed: number;
  api_calls_30d: number;
  avg_response_ms_30d: number;
  error_rate_30d: number;
  health_score: number;
  health_tier: string;
}
