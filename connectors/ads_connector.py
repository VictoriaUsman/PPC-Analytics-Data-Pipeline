"""Sponsored Ads v3 Reporting API connector (Sponsored Products / Brands / Display).

Implements the request/poll surface of connectors.base.AdsReportConnector against
`POST/GET https://{region-host}/reporting/reports[/{reportId}]`. See README's "Open Items"
for two live risks this connector inherits from the API itself, not from this code:
Sponsored Brands v3 only reports correctly for multi-ad-group-enabled campaigns, and
reports have been observed stuck in PENDING indefinitely (Amazon's own tracker, no
confirmed root cause) -- that's exactly what the poll loop's max-iteration timeout in
statemachine/ads_ingestion.asl.json guards against.
"""

from datetime import date

from connectors.base import AdsReportConnector, ReportStatus

REGION_HOSTS = {
    "NA": "advertising-api.amazon.com",
    "EU": "advertising-api-eu.amazon.com",
    "FE": "advertising-api-fe.amazon.com",
}

REPORT_TYPE_BY_AD_PRODUCT = {
    "SPONSORED_PRODUCTS": "spCampaigns",
    "SPONSORED_BRANDS": "sbCampaigns",
    "SPONSORED_DISPLAY": "sdCampaigns",
}

# Baseline column set common to all three ad products' campaign reports. Confirm against
# the live schema per ad product before relying on this in production -- Amazon's docs site
# is a JS-rendered SPA that resists automated scraping (see README), so this was assembled
# from the v3 request-shape examples in Amazon's public migration guide, not verified
# end-to-end against sandbox data.
COLUMNS_BY_AD_PRODUCT = {
    "SPONSORED_PRODUCTS": [
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    ],
    "SPONSORED_BRANDS": [
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    ],
    "SPONSORED_DISPLAY": [
        "date", "campaignId", "campaignName", "impressions", "clicks", "cost",
        "purchases14d", "sales14d",
    ],
}


class SponsoredAdsConnector(AdsReportConnector):
    def __init__(self, profile_id: str, region: str, access_token: str, client_id: str):
        super().__init__(profile_id, region, access_token, client_id)
        if region not in REGION_HOSTS:
            raise ValueError(f"unknown region: {region!r} (expected one of {tuple(REGION_HOSTS)})")
        self.host = REGION_HOSTS[region]

    def create_report(self, ad_product: str, start_date: date, end_date: date) -> str:
        report_type_id = REPORT_TYPE_BY_AD_PRODUCT[ad_product]
        body = {
            "name": f"{ad_product} campaigns {start_date.isoformat()}/{end_date.isoformat()}",
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "configuration": {
                "adProduct": ad_product,
                "groupBy": ["campaign"],
                "columns": COLUMNS_BY_AD_PRODUCT[ad_product],
                "reportTypeId": report_type_id,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }
        response = self._request(
            "POST",
            f"https://{self.host}/reporting/reports",
            json=body,
            headers={"Content-Type": "application/vnd.createasyncreportrequest.v3+json"},
        )
        response.raise_for_status()
        return response.json()["reportId"]

    def poll_report(self, report_id: str) -> ReportStatus:
        response = self._request("GET", f"https://{self.host}/reporting/reports/{report_id}")
        response.raise_for_status()
        payload = response.json()
        status = payload["status"]
        return ReportStatus(
            status=status,
            download_url=payload.get("url") if status == "COMPLETED" else None,
            failure_reason=payload.get("failureReason") if status == "FAILED" else None,
        )
