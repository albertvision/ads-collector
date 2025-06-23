import os
from google.ads.googleads.client import GoogleAdsClient

from .base import BaseProvider


class GoogleProvider(BaseProvider):
    name = "google"

    def __init__(self):
        self.config = os.getenv("GOOGLEADS_CONFIG")
        self.customer_id = os.getenv("GOOGLEADS_CUSTOMER_ID")
        if not self.config or not self.customer_id:
            raise RuntimeError("Google Ads configuration missing")
        self.client = GoogleAdsClient.load_from_storage(self.config)
        self.service = self.client.get_service("GoogleAdsService")

    def fetch_data(self, start_date, end_date):
        query = f"""
            SELECT
                customer.id,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                segments.date
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
        stream = self.service.search_stream(customer_id=self.customer_id, query=query)
        results = []
        for batch in stream:
            for row in batch.results:
                results.append({
                    "account_id": row.customer.id,
                    "campaign_id": row.campaign.id,
                    "campaign_name": row.campaign.name,
                    "adset_id": row.ad_group.id,
                    "adset_name": row.ad_group.name,
                    "ad_id": row.ad_group_ad.ad.id,
                    "ad_name": row.ad_group_ad.ad.name,
                    "spend": row.metrics.cost_micros / 1_000_000,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "date_start": str(row.segments.date),
                    "date_stop": str(row.segments.date),
                })
        return results
