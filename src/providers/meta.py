import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

import logging
from .base import BaseProvider
from ..utils import safe_api_call, get_dates_between


class MetaProvider(BaseProvider):
    name = "meta"

    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        account_ids_env = os.getenv("META_AD_ACCOUNT_ID")
        self.app_id = os.getenv("META_APP_ID")
        self.app_secret = os.getenv("META_APP_SECRET")
        self.logger = logging.getLogger(__name__)
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        if account_ids_env:
            self.ad_account_ids = [a.strip() for a in account_ids_env.split(",") if a.strip()]
        else:
            from facebook_business.adobjects.user import User
            user = User(fbid="me")
            accounts = safe_api_call(lambda: user.get_ad_accounts(fields=[AdAccount.Field.id]))
            self.ad_account_ids = [acc[AdAccount.Field.id] for acc in accounts]

    def fetch_data(self, start_date, end_date):
        data = []

        for account_id in self.ad_account_ids:
            account = AdAccount(account_id)

            for current_date in get_dates_between(start_date, end_date):
                params = {
                    "time_range": {
                        "since": current_date.isoformat(),
                        "until": current_date.isoformat(),
                    },
                    "level": "ad",
                    "fields": [
                        "account_id",
                        "campaign_id",
                        "campaign_name",
                        "adset_id",
                        "adset_name",
                        "ad_id",
                        "ad_name",
                        "spend",
                        "impressions",
                        "clicks",
                        "date_start",
                        "date_stop",
                    ],
                    "limit": 1000,
                }

                def get_data():
                    return account.get_insights(params=params)

                self.logger.info(
                    "Fetching data for ad account %s, date %s",
                    account_id,
                    current_date.isoformat(),
                )
                insights = safe_api_call(get_data)
                while True:
                    data += insights
                    if not insights.load_next_page():
                        break

        return data
